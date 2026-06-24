import {
  AutomergeUrl,
  Heads,
  NetworkAdapter,
  PeerId,
  Repo,
  type SubductionPolicy,
} from "@automerge/automerge-repo/slim";
import { hexToUint8Array, keyhiveIdentifierFromPeerId, uint8ArrayToHex } from "../utilities.js";
import { KeyhiveBlobInterceptor } from "./blob-interceptor.js";
import {
  Access,
  ChangeId,
  ContactCard,
  Document as KeyhiveDocument,
  DocumentId as KeyhiveDocumentId,
  Event as KeyhiveEvent,
  Identifier,
  Individual,
  Keyhive,
  Membership,
  Stats,
} from "@keyhive/keyhive/slim";
import { MemorySigner } from "@automerge/automerge-subduction/slim";
import { SyncServer } from "../sync-server.js";
import { Active } from "./active.js";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";
import { KeyhiveEventEmitter } from "./emitter.js";
import { docIdFromAutomergeUrl, KeyhiveStorage, receiveContactCard } from "./keyhive.js";
import { signData } from "../network-adapter/messages.js";
import { KeyhiveRustAdapter } from "../network-adapter/rust-transport/keyhive-rust-adapter.js";

// TODO: This is temporarily for calculating "best access". Move this and
// the best access method to WASM API.
const accessLevels: Record<string, number> = {
  None: 0,
  Relay: 1,
  Read: 2,
  Edit: 3,
  Admin: 4,
};

/**
 * Shared base for {@link AutomergeRepoKeyhive} and
 * {@link AutomergeRepoKeyhiveRust}. Holds the keyhive state common to both
 * paths and the membership/access operations that depend only on the
 * `Keyhive` instance.
 */
export abstract class AutomergeRepoKeyhiveBase {
  constructor(
    public readonly active: Active,
    public readonly keyhive: Keyhive,
    public readonly keyhiveStorage: KeyhiveStorage,
  ) {}

  // Build a subduction MemorySigner from this hive's Ed25519 key pair so
  // subduction and keyhive sign as the same peer. Requires that the
  // key pair was created extractable (which the default path in
  // `loadOrCreateSigner` ensures).
  async constructSubductionSigner(): Promise<MemorySigner> {
    const jwk = await crypto.subtle.exportKey(
      "jwk",
      this.active.keyPair.privateKey
    );
    if (!jwk.d) {
      throw new Error(
        "[AMRepoKeyhive] constructSubductionSigner: key pair has no private scalar (non-extractable?)"
      );
    }
    let b64 = jwk.d.replace(/-/g, "+").replace(/_/g, "/");
    const rem = b64.length % 4;
    if (rem) b64 += "=".repeat(4 - rem);
    const bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
    return MemorySigner.fromBytes(bytes);
  }

  async receiveContactCard(contactCard: ContactCard): Promise<Individual | undefined> {
    return receiveContactCard(this.keyhive, contactCard, this.keyhiveStorage);
  }

  async addMemberToDoc(
    docUrl: AutomergeUrl,
    contactCard: ContactCard,
    access: Access
  ) {
    await this.receiveContactCard(contactCard);
    const agent = await this.keyhive.getAgent(contactCard.id);
    if (!access || !agent) {
      console.error(
        "[AMRepoKeyhive] Failed to add member: invalid access or agent!"
      );
      return;
    }

    const docId: KeyhiveDocumentId = docIdFromAutomergeUrl(docUrl);
    console.debug(
      `[AMRepoKeyhive] addMemberToDoc: From url ${docUrl} derived Doc Id ${docId.toBytes()}`
    );
    const doc = await this.keyhive.getDocument(docId);
    if (!doc) {
      console.error(`[AMRepoKeyhive] Failed to add member: doc not found for id ${docId}`);
      return;
    }
    await this.keyhive.addMember(agent, doc.toMembered(), access, []);
  }

  async revokeMemberFromDoc(docUrl: AutomergeUrl, hexId: string) {
    const identifier = new Identifier(hexToUint8Array(hexId));
    const agent = await this.keyhive.getAgent(identifier);
    if (!agent) {
      console.error("[AMRepoKeyhive] Agent to revoke not found");
      return;
    }

    const docId = docIdFromAutomergeUrl(docUrl);
    const doc = await this.keyhive.getDocument(docId);
    if (!doc) {
      console.error(`[AMRepoKeyhive] Failed to revoke member: doc not found for id ${docId}`);
      return;
    }

    await this.keyhive.revokeMember(agent, true, doc.toMembered());
  }

  async setPublicAccess(docUrl: AutomergeUrl, access: Access) {
    const publicId = Identifier.publicId();
    const agent = await this.keyhive.getAgent(publicId);
    if (!agent) {
      console.error("[AMRepoKeyhive] Failed to get public agent");
      return;
    }

    const docId = docIdFromAutomergeUrl(docUrl);
    const doc = await this.keyhive.getDocument(docId);
    if (!doc) {
      console.error(`[AMRepoKeyhive] Failed to set public access: doc not found for id ${docId}`);
      return;
    }

    await this.keyhive.addMember(agent, doc.toMembered(), access, []);
  }

  async getPublicAccess(docUrl: AutomergeUrl): Promise<Access | undefined> {
    const publicId = Identifier.publicId();
    const docId = docIdFromAutomergeUrl(docUrl);
    return await this.keyhive.accessForDoc(publicId, docId);
  }

  async accessForDoc(id: Identifier, docId: KeyhiveDocumentId): Promise<Access | undefined> {
    return await this.keyhive.accessForDoc(id, docId);
  }

  async bestAccessForDoc(id: Identifier, docUrl: AutomergeUrl): Promise<Access | undefined> {
    const docId = docIdFromAutomergeUrl(docUrl);
    console.debug(`[AMRepoKeyhive] bestAccessForDoc: docId=${docId}`)
    const idAccess = await this.accessForDoc(id, docId);
    const idStr = idAccess ? idAccess.toString() : "None";
    const idAccessLevel = accessLevels[idStr];
    const publicId = Identifier.publicId();
    const publicAccess = await this.keyhive.accessForDoc(publicId, docId);
    const publicStr = publicAccess ? publicAccess.toString() : "None";
    const publicAccessLevel = accessLevels[publicStr];
    console.debug(`[AMRepoKeyhive] bestAccessForDoc: docId=${docId}, idStr=${idStr}, publicStr=${publicStr}, idAccessLevel=${idAccessLevel}, publicAccessLevel=${publicAccessLevel}`);
    return (idAccessLevel > publicAccessLevel) ? idAccess : publicAccess;
  }

  async docMemberCapabilities(docId: KeyhiveDocumentId): Promise<Membership[]> {
    return await this.keyhive.docMemberCapabilities(docId);
  }

  async stats(): Promise<Stats> {
    return await this.keyhive.stats();
  }
}

export class AutomergeRepoKeyhive extends AutomergeRepoKeyhiveBase {
  #linked = false;

  constructor(
    active: Active,
    keyhive: Keyhive,
    keyhiveStorage: KeyhiveStorage,
    public readonly peerId: PeerId,
    public readonly syncServer: SyncServer,
    public readonly networkAdapter: KeyhiveNetworkAdapter,
    public readonly emitter: KeyhiveEventEmitter,
    public readonly idFactory: (heads: Heads) => Promise<Uint8Array>,
    public readonly createKeyhiveNetworkAdapter: (networkAdapter: NetworkAdapter, onlyShareWithHardcodedServerPeerId: boolean, periodicallyRequestSync: boolean, syncRequestInterval: number, batchInterval?: number, archiveThreshold?: number) => KeyhiveNetworkAdapter,
  ) {
    super(active, keyhive, keyhiveStorage);
  }

  // Configure `AutomergeRepoKeyhive` to notify the provided `Repo` about
  // potential `Keyhive` membership updates. Debounces ingest-remote events
  // so that bursts of keyhive ops don't trigger sweeps on every single event.
  linkRepo(repo: Repo, options?: { debounceMs?: number, onBeforeShareConfigChanged?: () => void }) {
    if (this.#linked) {
      console.warn("[AMRepoKeyhive] linkRepo called more than once; ignoring.")
      return
    }
    this.#linked = true
    const debounceMs = options?.debounceMs ?? 2000
    const onBefore = options?.onBeforeShareConfigChanged
    let timer: ReturnType<typeof setTimeout> | null = null
    let inProgress = false;

    (this.networkAdapter as any).on("ingest-remote", () => {
      inProgress = true
      if (timer) return
      timer = setTimeout(() => {
        timer = null
        if (!inProgress) return
        inProgress = false
        try {
          onBefore?.()
          repo.shareConfigChanged()
        } catch (e) {
          console.error(`[AMRepoKeyhive] shareConfigChanged() threw:`, e)
        }
      }, debounceMs)
    })
  }

  buildServerSubductionPolicy(): SubductionPolicy {
    const keyhive = this.keyhive;

    // Legacy (non-keyhive) doc IDs are 16 bytes padded to 32 with zeros.
    const isLegacyDocId = (bytes: Uint8Array): boolean => {
      for (let i = 16; i < 32; i++) {
        if (bytes[i] !== 0) return false;
      }
      return true;
    };

    const hasAccess = async (id: Identifier, docId: KeyhiveDocumentId, minLevel: number): Promise<boolean> => {
      try {
        // Check public access
        const publicAccess = await keyhive.accessForDoc(Identifier.publicId(), docId);
        const publicStr = publicAccess ? publicAccess.toString() : "None";
        if (accessLevels[publicStr] >= minLevel) return true;

        // Check direct access
        const access = await keyhive.accessForDoc(id, docId);
        const accessStr = access ? access.toString() : "None";
        return accessLevels[accessStr] >= minLevel;
      } catch (e) {
        console.error(`[SubductionPolicy] hasAccess threw for docId=${docId}:`, e);
        return false;
      }
    };

    return {
      async authorizeConnect(_peerId) {
        // Allow all connections. Doc checks (authorizeFetch,
        // authorizePut, filterAuthorizedFetch) determine actual access.
      },

      async authorizeFetch(peerId, sedimentreeId) {
        const sidBytes = sedimentreeId.toBytes();
        if (isLegacyDocId(sidBytes)) return;
        const identifier = new Identifier(peerId.toBytes());
        const docId = new KeyhiveDocumentId(sidBytes);
        if (!(await hasAccess(identifier, docId, accessLevels.Relay))) {
          throw new Error("insufficient access to fetch: requires at least Relay");
        }
      },

      async authorizePut(_requestor, author, sedimentreeId) {
        const sidBytes = sedimentreeId.toBytes();
        if (isLegacyDocId(sidBytes)) return;
        const identifier = new Identifier(author.toBytes());
        const docId = new KeyhiveDocumentId(sidBytes);
        if (!(await hasAccess(identifier, docId, accessLevels.Edit))) {
          throw new Error("insufficient access to put: requires at least Edit");
        }
      },

      async filterAuthorizedFetch(peerId, ids) {
        const identifier = new Identifier(peerId.toBytes());
        const authorized = [];
        for (const sid of ids) {
          const sidBytes = sid.toBytes();
          if (isLegacyDocId(sidBytes)) {
            authorized.push(sid);
            continue;
          }
          const docId = new KeyhiveDocumentId(sidBytes);
          if (await hasAccess(identifier, docId, accessLevels.Relay)) {
            authorized.push(sid);
          }
        }
        return authorized;
      },
    };
  }

  // @deprecated Use {@link addSyncServerRelayToDoc} instead.
  async addSyncServerPullToDoc(docUrl: AutomergeUrl) {
    return this.addSyncServerRelayToDoc(docUrl);
  }

  async addSyncServerRelayToDoc(docUrl: AutomergeUrl) {
    if (!this.syncServer) return;
    try {
      const serverContactCard = ContactCard.fromJson(
        this.syncServer.contactCard.toJson()
      );
      if (!serverContactCard) {
        console.error("[AMRepoKeyhive] Failed to parse sync server contact card");
        return;
      }
      const relayAccess = Access.tryFromString("relay");
      if (!relayAccess) {
        console.error("[AMRepoKeyhive] Failed to create Relay access");
        return;
      }
      await this.addMemberToDoc(docUrl, serverContactCard, relayAccess);
    } catch (err) {
      console.error("[AMRepoKeyhive] Failed to add sync server to doc:", err);
    }
  }

  async generateDoc(): Promise<KeyhiveDocument> {
    return generateDoc(this.keyhive);
  }

  async signData(
    data: Uint8Array,
    contactCard?: ContactCard
  ): Promise<Uint8Array> {
    return signData(this.keyhive, data, contactCard);
  }

  keyhiveIdFactory(): (heads: Heads) => Promise<Uint8Array> {
    return keyhiveIdFactory(this.networkAdapter, this.keyhive)
  }
};

export async function generateDoc(kh: Keyhive): Promise<KeyhiveDocument> {
  // For now, randomly generate a ChangeId
  const changeIdArray = crypto.getRandomValues(new Uint8Array(10));
  const changeId = new ChangeId(changeIdArray);
  const g = await kh.generateGroup([]);
  const doc = await kh.generateDocument([g.toPeer()], changeId, []);
  console.debug(
    `[AMRepoKeyhive] Generated Keyhive document with id ${doc.doc_id.toBytes()}`
  );
  return doc;
}

export function keyhiveIdFactory(_keyhiveNetworkAdapter: KeyhiveNetworkAdapter, keyhive: Keyhive): (heads: Heads) => Promise <Uint8Array> {
  return async (_heads: Heads) => {
    const doc = await generateDoc(keyhive);
    return doc.doc_id.toBytes();
  };
}

/**
 * Counterpart to {@link AutomergeRepoKeyhive} for peers that talk to a
 * Rust subduction-keyhive sync server.
 */
export class AutomergeRepoKeyhiveRust extends AutomergeRepoKeyhiveBase {
  #linkRepoSchedule: (() => void) | null = null;
  #repo: Repo | null = null;
  // Per-doc set of member-id hexes already seen. A membership *increase* caused
  // by our own agent triggers a single rotation + nudge edit, giving the new
  // member an entry point into the document's predecessor-secret chain.
  #anchoredMembers: Map<string, Set<string>> = new Map();

  constructor(
    active: Active,
    keyhive: Keyhive,
    keyhiveStorage: KeyhiveStorage,
    public readonly peerId: PeerId,
    public readonly emitter: KeyhiveEventEmitter,
    public readonly networkAdapter: KeyhiveRustAdapter,
    public readonly idFactory: (heads: Heads) => Promise<Uint8Array>,
    public readonly createKeyhiveNetworkAdapter: (
      networkAdapter: NetworkAdapter,
      onlyShareWithHardcodedServerPeerId: boolean,
      periodicallyRequestSync: boolean,
      syncRequestInterval: number,
      batchInterval?: number,
      archiveThreshold?: number,
    ) => KeyhiveNetworkAdapter,
    public readonly blobInterceptor: KeyhiveBlobInterceptor,
  ) {
    super(active, keyhive, keyhiveStorage);
  }

  /** Wire keyhive membership updates to {@link Repo.shareConfigChanged}(). */
  linkRepo(repo: Repo, options?: { debounceMs?: number; onBeforeShareConfigChanged?: () => void }) {
    if (this.#linkRepoSchedule) {
      console.warn("[AMRepoKeyhiveRust] linkRepo called more than once; ignoring.");
      return;
    }
    const debounceMs = options?.debounceMs ?? 2000;
    const onBefore = options?.onBeforeShareConfigChanged;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let pending = false;

    const schedule = () => {
      pending = true;
      if (timer) return;
      timer = setTimeout(() => {
        timer = null;
        if (!pending) return;
        pending = false;
        try {
          onBefore?.();
          repo.shareConfigChanged();
        } catch (e) {
          console.error(`[AMRepoKeyhiveRust] shareConfigChanged() threw:`, e);
        }
        // Membership may have changed; nudge any reader our agent just added.
        void this.#checkForMembershipNudges();
      }, debounceMs);
    };

    this.#repo = repo;
    this.#linkRepoSchedule = schedule;
    this.emitter.on("update", schedule);
    (this.networkAdapter as any).on("ingest-remote", schedule);
  }

  /**
   * Signal a keyhive change that originated from this same agent (membership
   * or content), scheduling a debounced `shareConfigChanged()` (which also runs
   * the membership-nudge check).
   */
  notifySameAgentKeyhiveChange(): void {
    this.#linkRepoSchedule?.();
  }

  /**
   * Give any reader our agent just added a way into the document's history.
   *
   * A bare add leaves the new member with no derivable epoch key, so we rotate
   * the PCS key (`forcePcsUpdate`) and then write a tiny nudge edit under it.
   * That edit encrypts under the post-rotation key and carries the
   * predecessor-secret chain back through the current heads, so the new member
   * can CGKA-derive the nudge's key once and then walk the whole prior history.
   *
   * Only the encrypting instance runs this (it is the one that holds the repo
   * and the blob interceptor), and only for members WE added — a remote add is
   * the adder's responsibility to nudge.
   */
  async #checkForMembershipNudges(): Promise<void> {
    const repo = this.#repo;
    if (!repo) return;
    // Zero-padded 64-hex id, matching the members' `who.id` / `verifyingKey`
    // hexes. `keyhive.idString` is not zero-padded, so it must not be used here.
    const ourIdHex = uint8ArrayToHex(this.keyhive.id.bytes);
    for (const documentId of this.blobInterceptor.trackedDocIds) {
      try {
        const doc = await this.keyhive.getDocument(
          docIdFromAutomergeUrl(`automerge:${documentId}` as AutomergeUrl),
        );
        if (!doc) continue;
        const members = await doc.members();
        const currentIds = new Set(
          members.map((c) => uint8ArrayToHex(c.who.id.toBytes())),
        );
        // First sighting: seed the baseline with only members we did NOT add,
        // so our own adds present at first sight are still nudged below (seeding
        // the full membership would swallow a create-then-share in one window).
        const baseline =
          this.#anchoredMembers.get(documentId) ??
          new Set(
            members
              .filter((c) => uint8ArrayToHex(c.proof.verifyingKey) !== ourIdHex)
              .map((c) => uint8ArrayToHex(c.who.id.toBytes())),
          );
        let addedByUs = 0;
        for (const c of members) {
          const idHex = uint8ArrayToHex(c.who.id.toBytes());
          if (idHex === ourIdHex) continue; // never nudge for our own membership
          if (baseline.has(idHex)) continue; // not new
          if (uint8ArrayToHex(c.proof.verifyingKey) === ourIdHex) addedByUs++;
        }
        // Record the new membership regardless, so a given add nudges once.
        this.#anchoredMembers.set(documentId, currentIds);
        if (addedByUs === 0) continue;

        // Rotate (so the new member has a derivable current key), persist the
        // rotated leaf secret for any sibling instance, then write the nudge.
        const leafSecret = await this.keyhive.forcePcsUpdate(doc);
        await this.keyhiveStorage.saveLeafSecret(leafSecret);
        const handle = await repo.find(`automerge:${documentId}` as AutomergeUrl);
        handle.change((d: any) => {
          d._lastAddMember = Date.now();
        });
      } catch (e) {
        console.error(
          `[AMRepoKeyhiveRust] membership nudge check failed for ${documentId}:`,
          e,
        );
      }
    }
  }

  async addSyncServerRelayToDoc(docUrl: AutomergeUrl) {
    const identifier = keyhiveIdentifierFromPeerId(this.networkAdapter.remotePeerId);
    const agent = await this.keyhive.getAgent(identifier);
    if (!agent) {
      throw new Error(
        `[AMRepoKeyhiveRust] sync server agent not yet known; retry after sync. remotePeerId=${this.networkAdapter.remotePeerId}`,
      );
    }
    try {
      const relayAccess = Access.tryFromString("relay");
      if (!relayAccess) {
        console.error("[AMRepoKeyhiveRust] Failed to create Relay access");
        return;
      }
      const docId = docIdFromAutomergeUrl(docUrl);
      const doc = await this.keyhive.getDocument(docId);
      if (!doc) {
        console.error(`[AMRepoKeyhiveRust] Failed to add sync server relay: doc not found for id ${docId}`);
        return;
      }
      await this.keyhive.addMember(agent, doc.toMembered(), relayAccess, []);
    } catch (err) {
      console.error("[AMRepoKeyhiveRust] Failed to add sync server to doc:", err);
    }
  }
}
