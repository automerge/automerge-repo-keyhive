import { log } from "../logging.js";
import {
  AutomergeUrl,
  Heads,
  NetworkAdapter,
  PeerId,
  Repo,
  type SubductionPolicy,
} from "@automerge/automerge-repo/slim";
import {
  hexToUint8Array,
  isUnprotectedDoc,
  isUnprotectedDocId,
  keyhiveIdentifierFromPeerId,
  UnprotectedDocError,
  uint8ArrayToHex,
} from "../utilities.js";
import { KeyhiveBlobInterceptor } from "./blob-interceptor.js";
import {
  Access,
  Capability,
  ChangeId,
  ContactCard,
  Document as KeyhiveDocument,
  DocumentId as KeyhiveDocumentId,
  GroupId,
  Identifier,
  Individual,
  Keyhive,
  Membership,
  Stats,
} from "@keyhive/keyhive/slim";
import { MemorySigner } from "@automerge/automerge-subduction/slim";
import type { SyncServer } from "../sync-server.js";
import { Active } from "./active.js";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";
import { KeyhiveEventEmitter } from "./emitter.js";
import {
  docIdFromAutomergeUrl,
  KeyhiveStorage,
  receiveContactCard,
} from "./keyhive.js";
import { signData } from "../network-adapter/messages.js";
import { KeyhiveSubductionAdapter } from "../network-adapter/subduction-transport/keyhive-subduction-adapter.js";

export type CreateKeyhiveNetworkAdapter = (
  networkAdapter: NetworkAdapter,
  options?: WrapNetworkAdapterOptions
) => KeyhiveNetworkAdapter;

/**
 * Field written into a document by the membership "nudge" edit.
 */
export const NUDGE_FIELD = "__automerge-repo-keyhive__last-added-member-ts";

/**
 * Shared base for {@link LegacyAutomergeRepoKeyhive} and
 * {@link AutomergeRepoKeyhive}. Holds the keyhive state common to
 * operations that depend only on the `Keyhive` instance.
 */
export abstract class AutomergeRepoKeyhiveBase {
  /**
   * Each path narrows this to its own adapter type. Only `disconnect` is
   * needed here, for {@link close}.
   */
  abstract readonly networkAdapter: { disconnect(): void };

  /**
   * Debounce timer for the `shareConfigChanged` notification scheduled by
   * `linkRepo`.
   */
  protected shareConfigTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * Cleanup callbacks for resources wired up before this instance existed.
   * The init functions attach the keyhive event-flush listener to the shared
   * emitter while building the hive, and its pending sync timer has to be
   * cancelled by {@link close}. Run once, then dropped.
   */
  private cleanups: Array<() => void> = [];

  constructor(
    public readonly active: Active,
    public readonly keyhive: Keyhive,
    public readonly keyhiveStorage: KeyhiveStorage,
    public readonly emitter: KeyhiveEventEmitter
  ) {}

  /**
   * Register a cleanup callback to run on {@link close}.
   *
   * @internal Called by the init functions for teardown that has to be set up
   * before the hive instance exists.
   */
  registerCleanup(cleanup: () => void): void {
    this.cleanups.push(cleanup);
  }

  /**
   * Cancel the pending share-config notification, run registered cleanups,
   * remove all emitter listeners, and disconnect the network adapter, which
   * stops its timers. The hive is unusable afterwards.
   */
  close(): void {
    if (this.shareConfigTimer) {
      clearTimeout(this.shareConfigTimer);
      this.shareConfigTimer = null;
    }
    const cleanups = this.cleanups;
    this.cleanups = [];
    for (const cleanup of cleanups) {
      cleanup();
    }
    this.networkAdapter.disconnect();
    this.emitter.removeAllListeners();
  }

  /**
   * Called after this instance changes a document's membership locally.
   * Default implementation is a no-op.
   */
  protected noteLocalMembershipChange(_docUrl: AutomergeUrl): void {}

  /**
   * Build a subduction MemorySigner from this hive's Ed25519 key pair so
   * subduction and keyhive sign as the same peer. Requires that the
   * key pair was created extractable (which the default path in
   * `loadOrCreateSigner` ensures).
   *
   * @internal Called by the init functions.
   */
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

  async receiveContactCard(
    contactCard: ContactCard
  ): Promise<Individual | undefined> {
    return receiveContactCard(this.keyhive, contactCard, this.keyhiveStorage);
  }

  /** Return the document for `docUrl` or throw a descriptive error noting the operation. */
  private async getDocForOperation(
    operation: string,
    docUrl: AutomergeUrl
  ): Promise<KeyhiveDocument> {
    const docId = docIdFromAutomergeUrl(docUrl);
    const doc = await this.keyhive.getDocument(docId);
    if (!doc) {
      throw new Error(
        `${operation}: document not found in keyhive for ${docUrl} (has it synced yet?)`
      );
    }
    return doc;
  }

  /**
   * Grant `access` on the document to the identity in `contactCard`.
   *
   * Throws if the document is unprotected, is not known to
   * keyhive, or the contact card does not resolve to an agent.
   */
  async addMemberToDoc(
    docUrl: AutomergeUrl,
    contactCard: ContactCard,
    access: Access
  ): Promise<void> {
    if (isUnprotectedDoc(docUrl)) {
      throw new UnprotectedDocError("addMemberToDoc", docUrl);
    }
    await this.receiveContactCard(contactCard);
    const agent = await this.keyhive.getAgent(contactCard.id);
    if (!agent) {
      throw new Error(
        `addMemberToDoc: contact card did not resolve to a keyhive agent (id ${uint8ArrayToHex(contactCard.id.toBytes())})`
      );
    }

    const doc = await this.getDocForOperation("addMemberToDoc", docUrl);
    await this.keyhive.addMember(agent, doc.toMembered(), access, []);
    this.noteLocalMembershipChange(docUrl);
  }

  /**
   * Revoke a member's access to the document.
   *
   * `member` is an `Identifier` or its hex-encoded bytes (the `id` returned
   * by {@link listMembers}). Throws if the document is unprotected,
   * is not known to keyhive, or the member does not resolve to an agent.
   */
  async revokeMemberFromDoc(
    docUrl: AutomergeUrl,
    member: Identifier | string
  ): Promise<void> {
    if (isUnprotectedDoc(docUrl)) {
      throw new UnprotectedDocError("revokeMemberFromDoc", docUrl);
    }
    const identifier =
      typeof member === "string"
        ? new Identifier(hexToUint8Array(member))
        : member;
    const agent = await this.keyhive.getAgent(identifier);
    if (!agent) {
      throw new Error(
        `revokeMemberFromDoc: member not found in keyhive (id ${uint8ArrayToHex(identifier.toBytes())})`
      );
    }

    const doc = await this.getDocForOperation("revokeMemberFromDoc", docUrl);
    await this.keyhive.revokeMember(agent, true, doc.toMembered());
  }

  /**
   * Grant `access` on the document to everyone via the special public
   * member. Throws if the document is unprotected or is not known to
   * keyhive.
   */
  async setPublicAccess(docUrl: AutomergeUrl, access: Access): Promise<void> {
    if (isUnprotectedDoc(docUrl)) {
      throw new UnprotectedDocError("setPublicAccess", docUrl);
    }
    const publicId = Identifier.publicId();
    const agent = await this.keyhive.getAgent(publicId);
    if (!agent) {
      throw new Error("setPublicAccess: public agent not found in keyhive");
    }

    const doc = await this.getDocForOperation("setPublicAccess", docUrl);
    await this.keyhive.addMember(agent, doc.toMembered(), access, []);
    this.noteLocalMembershipChange(docUrl);
  }

  async getPublicAccess(docUrl: AutomergeUrl): Promise<Access | undefined> {
    if (isUnprotectedDoc(docUrl)) return undefined;
    const publicId = Identifier.publicId();
    const docId = docIdFromAutomergeUrl(docUrl);
    return await this.keyhive.accessForDoc(publicId, docId);
  }

  /**
   * The document's keyhive id. Accepts a `DocumentId` as well as an
   * `AutomergeUrl` because tool bundles built against 0.3 still pass an
   * `DocumentId`.
   */
  private toDocId(
    docUrlOrId: AutomergeUrl | KeyhiveDocumentId
  ): KeyhiveDocumentId {
    return typeof docUrlOrId === "string"
      ? docIdFromAutomergeUrl(docUrlOrId)
      : docUrlOrId;
  }

  /** `id`'s direct access to the document or undefined if none (including for unprotected docs). */
  async accessForDoc(
    id: Identifier,
    docUrl: AutomergeUrl | KeyhiveDocumentId
  ): Promise<Access | undefined> {
    const docId = this.toDocId(docUrl);
    if (isUnprotectedDocId(docId.toBytes())) return undefined;
    return await this.keyhive.accessForDoc(id, docId);
  }

  /**
   * The most permissive of `id`'s direct access and the document's public
   * access, or undefined if none (including for unprotected docs).
   */
  async bestAccessForDoc(
    id: Identifier,
    docUrl: AutomergeUrl
  ): Promise<Access | undefined> {
    if (isUnprotectedDoc(docUrl)) return undefined;
    return await this.keyhive.bestAccessForDoc(
      id,
      docIdFromAutomergeUrl(docUrl)
    );
  }

  /** All member capabilities for the document (empty for unprotected docs). */
  async docMemberCapabilities(
    docUrl: AutomergeUrl | KeyhiveDocumentId
  ): Promise<Membership[]> {
    const docId = this.toDocId(docUrl);
    if (isUnprotectedDocId(docId.toBytes())) return [];
    return await this.keyhive.docMemberCapabilities(docId);
  }

  /**
   * All members of the document (empty for unprotected docs).
   */
  async listMembers(docUrl: AutomergeUrl): Promise<DocMember[]> {
    const capabilities = await this.docMemberCapabilities(docUrl);
    const selfHex = uint8ArrayToHex(this.active.individual.id.toBytes());
    const publicHex = uint8ArrayToHex(Identifier.publicId().toBytes());
    const serverHex = this.syncServerIdentifierHex();
    return capabilities.map((capability) => {
      const id = uint8ArrayToHex(capability.who.id.toBytes());
      return {
        id,
        access: capability.can,
        isSelf: id === selfHex,
        isPublic: id === publicHex,
        isSyncServer: serverHex !== null && id === serverHex,
      };
    });
  }

  /**
   * Hex-encoded identifier of the sync server configured for this hive or
   * null if unknown. Used to tag the server in {@link listMembers}.
   */
  protected abstract syncServerIdentifierHex(): string | null;

  /** Grant the configured sync server relay access to a document. */
  abstract addSyncServerRelayToDoc(docUrl: AutomergeUrl): Promise<void>;

  /**
   * @deprecated Kept only for prebuilt bundles compiled against 0.3. Renamed
   * to {@link addSyncServerRelayToDoc}, which this calls under the hood.
   */
  async addSyncServerPullToDoc(docUrl: AutomergeUrl): Promise<void> {
    return this.addSyncServerRelayToDoc(docUrl);
  }

  async stats(): Promise<Stats> {
    return await this.keyhive.stats();
  }
}

/** One document member as returned by {@link AutomergeRepoKeyhiveBase.listMembers}. */
export interface DocMember {
  /** Hex-encoded Identifier bytes. Accepted by revokeMemberFromDoc. */
  id: string;
  /** This member's access level to the doc. */
  access: Access;
  /** Returns true if this member is this hive's own identity. */
  isSelf: boolean;
  /** Returns true if this member is the special public member. */
  isPublic: boolean;
  /** Returns true if this member is the sync server configured for this hive. */
  isSyncServer: boolean;
}

/**
 * Implementation of {@link AutomergeRepoKeyhiveBase} for legacy sync peers.
 */
export class LegacyAutomergeRepoKeyhive extends AutomergeRepoKeyhiveBase {
  #linked = false;

  constructor(
    active: Active,
    keyhive: Keyhive,
    keyhiveStorage: KeyhiveStorage,
    public readonly peerId: PeerId,
    /** Null when initialized with syncServer "none". */
    public readonly syncServer: SyncServer | null,
    public readonly networkAdapter: KeyhiveNetworkAdapter,
    emitter: KeyhiveEventEmitter,
    public readonly idFactory: (heads: Heads) => Promise<Uint8Array>,
    public readonly createKeyhiveNetworkAdapter: CreateKeyhiveNetworkAdapter
  ) {
    super(active, keyhive, keyhiveStorage, emitter);
  }

  /**
   * Configure `LegacyAutomergeRepoKeyhive` to notify the provided `Repo` about
   * potential `Keyhive` membership updates. Debounces ingest-remote events.
   *
   * @internal Called by the init functions. Not part of the public API.
   */
  linkRepo(
    repo: Repo,
    options?: { debounceMs?: number; onBeforeShareConfigChanged?: () => void }
  ) {
    if (this.#linked) {
      log.warn("[AMRepoKeyhive] linkRepo called more than once; ignoring.");
      return;
    }
    this.#linked = true;
    const debounceMs = options?.debounceMs ?? 2000;
    const onBefore = options?.onBeforeShareConfigChanged;
    let inProgress = false;

    this.networkAdapter.on("ingest-remote", () => {
      inProgress = true;
      if (this.shareConfigTimer) return;
      this.shareConfigTimer = setTimeout(() => {
        this.shareConfigTimer = null;
        if (!inProgress) return;
        inProgress = false;
        try {
          onBefore?.();
          repo.shareConfigChanged();
        } catch (e) {
          log.error(`[AMRepoKeyhive] shareConfigChanged() threw:`, e);
        }
      }, debounceMs);
    });
  }

  buildServerSubductionPolicy(): SubductionPolicy {
    const keyhive = this.keyhive;

    const hasAccess = async (
      id: Identifier,
      docId: KeyhiveDocumentId,
      minAccess: Access
    ): Promise<boolean> => {
      try {
        const best = await keyhive.bestAccessForDoc(id, docId);
        return best !== undefined && best.atLeast(minAccess);
      } catch (e) {
        log.error(`[SubductionPolicy] hasAccess threw for docId=${docId}:`, e);
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
        if (isUnprotectedDocId(sidBytes)) return;
        const identifier = new Identifier(peerId.toBytes());
        const docId = new KeyhiveDocumentId(sidBytes);
        if (!(await hasAccess(identifier, docId, Access.relay()))) {
          throw new Error(
            "insufficient access to fetch: requires at least Relay"
          );
        }
      },

      async authorizePut(_requestor, author, sedimentreeId) {
        const sidBytes = sedimentreeId.toBytes();
        if (isUnprotectedDocId(sidBytes)) return;
        const identifier = new Identifier(author.toBytes());
        const docId = new KeyhiveDocumentId(sidBytes);
        if (!(await hasAccess(identifier, docId, Access.edit()))) {
          throw new Error("insufficient access to put: requires at least Edit");
        }
      },

      async filterAuthorizedFetch(peerId, ids) {
        const identifier = new Identifier(peerId.toBytes());
        const authorized = [];
        for (const sid of ids) {
          const sidBytes = sid.toBytes();
          if (isUnprotectedDocId(sidBytes)) {
            authorized.push(sid);
            continue;
          }
          const docId = new KeyhiveDocumentId(sidBytes);
          if (await hasAccess(identifier, docId, Access.relay())) {
            authorized.push(sid);
          }
        }
        return authorized;
      },
    };
  }

  /**
   * Grant the sync server relay access to the document so it can sync the
   * ciphertext without being able to read it. Throws on failure.
   */
  async addSyncServerRelayToDoc(docUrl: AutomergeUrl): Promise<void> {
    if (isUnprotectedDoc(docUrl)) {
      throw new UnprotectedDocError("addSyncServerRelayToDoc", docUrl);
    }
    if (!this.syncServer) {
      throw new Error(
        'addSyncServerRelayToDoc: no sync server configured (syncServer is "none")'
      );
    }
    const serverContactCard = ContactCard.fromJson(
      this.syncServer.contactCard.toJson()
    );
    if (!serverContactCard) {
      throw new Error(
        "addSyncServerRelayToDoc: failed to parse sync server contact card"
      );
    }
    await this.addMemberToDoc(docUrl, serverContactCard, Access.relay());
  }

  protected syncServerIdentifierHex(): string | null {
    return this.syncServer
      ? uint8ArrayToHex(this.syncServer.individualId)
      : null;
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
    return keyhiveIdFactory(this.networkAdapter, this.keyhive);
  }
}

export async function generateDoc(kh: Keyhive): Promise<KeyhiveDocument> {
  // For now, randomly generate a ChangeId
  const changeIdArray = crypto.getRandomValues(new Uint8Array(10));
  const changeId = new ChangeId(changeIdArray);
  const g = await kh.generateGroup([]);
  const doc = await kh.generateDocument([g.toPeer()], changeId, []);
  log.debug(
    `[AMRepoKeyhive] Generated Keyhive document with id ${doc.doc_id.toBytes()}`
  );
  return doc;
}

export function keyhiveIdFactory(
  _keyhiveNetworkAdapter: KeyhiveNetworkAdapter,
  keyhive: Keyhive
): (heads: Heads) => Promise<Uint8Array> {
  return async (_heads: Heads) => {
    const doc = await generateDoc(keyhive);
    return doc.doc_id.toBytes();
  };
}

/**
 * Implementation of {@link AutomergeRepoKeyhiveBase} for subduction peers.
 */
export class AutomergeRepoKeyhive extends AutomergeRepoKeyhiveBase {
  #linkRepoSchedule: (() => void) | null = null;
  #repo: Repo | null = null;
  // Per-doc set of member id hexes already seen. A membership increase caused
  // by our own agent triggers a single rotation and nudge edit, giving the new
  // member an entry point into the document's predecessor app secret chain.
  #seenMembers: Map<string, Set<string>> = new Map();
  // Document ids (the string after "automerge:") whose membership this
  // instance changed locally and the nudge check has not yet examined.
  #docsWithLocalMembershipChanges: Set<string> = new Set();

  constructor(
    active: Active,
    keyhive: Keyhive,
    keyhiveStorage: KeyhiveStorage,
    public readonly peerId: PeerId,
    emitter: KeyhiveEventEmitter,
    public readonly networkAdapter: KeyhiveSubductionAdapter,
    public readonly idFactory: (heads: Heads) => Promise<Uint8Array>,
    public readonly createKeyhiveNetworkAdapter: CreateKeyhiveNetworkAdapter,
    public readonly blobInterceptor: KeyhiveBlobInterceptor
  ) {
    super(active, keyhive, keyhiveStorage, emitter);
  }

  protected override noteLocalMembershipChange(docUrl: AutomergeUrl): void {
    this.#docsWithLocalMembershipChanges.add(docUrl.replace(/^automerge:/, ""));
  }

  /**
   * Like {@link AutomergeRepoKeyhiveBase.close}, additionally dropping the blob
   * interceptor's cached key material.
   */
  override close(): void {
    super.close();
    this.blobInterceptor.clearCachedKeyMaterial();
  }

  /**
   * Wire keyhive membership updates to {@link Repo.shareConfigChanged}().
   *
   * @internal Called by the init functions.
   */
  linkRepo(
    repo: Repo,
    options?: { debounceMs?: number; onBeforeShareConfigChanged?: () => void }
  ) {
    if (this.#linkRepoSchedule) {
      log.warn(
        "[AMRepoKeyhiveSubduction] linkRepo called more than once; ignoring."
      );
      return;
    }
    const debounceMs = options?.debounceMs ?? 2000;
    const onBefore = options?.onBeforeShareConfigChanged;
    let pending = false;

    const schedule = () => {
      pending = true;
      if (this.shareConfigTimer) return;
      this.shareConfigTimer = setTimeout(() => {
        this.shareConfigTimer = null;
        if (!pending) return;
        pending = false;
        try {
          onBefore?.();
          repo.shareConfigChanged();
        } catch (e) {
          log.error(`[AMRepoKeyhiveSubduction] shareConfigChanged() threw:`, e);
        }
        // Membership may have changed. Nudge any reader our agent just added.
        void this.#checkForMembershipNudges();
      }, debounceMs);
    };

    this.#repo = repo;
    this.#linkRepoSchedule = schedule;
    this.emitter.on("update", schedule);
    this.networkAdapter.on("ingest-remote", schedule);
  }

  /**
   * Signal a keyhive change that originated from this same agent.
   */
  notifySameAgentKeyhiveChange(): void {
    this.#linkRepoSchedule?.();
  }

  /**
   * Everyone we granted access to, anywhere under this document's delegations.
   */
  async #individualsWeGrantedAccessTo(
    doc: KeyhiveDocument,
    ourIdHex: string
  ): Promise<Set<string>> {
    const granted = new Set<string>();
    const seenGroups = new Set<string>();

    const walk = async (
      capabilities: Capability[],
      grantedByUs: boolean
    ): Promise<void> => {
      for (const capability of capabilities) {
        const byUs =
          grantedByUs ||
          uint8ArrayToHex(capability.proof.verifyingKey) === ourIdHex;
        const idHex = uint8ArrayToHex(capability.who.id.toBytes());
        if (capability.who.isGroup()) {
          // A cycle would otherwise not terminate.
          if (seenGroups.has(idHex)) continue;
          seenGroups.add(idHex);
          const group = await this.keyhive.getGroup(
            new GroupId(capability.who.id.toBytes())
          );
          if (group) await walk(await group.members(), byUs);
          continue;
        }
        if (byUs) granted.add(idHex);
      }
    };

    await walk(await doc.members(), false);
    return granted;
  }

  /**
   * Give any reader our agent just added a way into the document's history.
   *
   * A simple add leaves the new member with no derivable key, so we rotate
   * the PCS key via `forcePcsUpdate` and then write a nudge edit under it.
   * That edit encrypts under the post-rotation key and gives a way into the
   * predecessor app secret chain for the new member.
   *
   * Who counts as new is decided by the document's CGKA membership, not by its
   * delegations. Adding someone to a group that already has access puts them
   * in the CGKA while the document's own members are unchanged.
   */
  async #checkForMembershipNudges(): Promise<void> {
    const repo = this.#repo;
    if (!repo) return;
    // Zero-padded 64-hex id, matching the members' `who.id` and `verifyingKey`
    // hexes.
    const ourIdHex = uint8ArrayToHex(this.keyhive.id.bytes);
    const docIds = new Set([
      ...this.blobInterceptor.trackedDocIds,
      ...this.#docsWithLocalMembershipChanges,
      // Never encrypted, so absent from trackedDocIds.
      ...this.blobInterceptor.docIdsAwaitingPcsKey,
    ]);
    for (const documentId of docIds) {
      try {
        const doc = await this.keyhive.getDocument(
          docIdFromAutomergeUrl(`automerge:${documentId}` as AutomergeUrl)
        );
        if (!doc) {
          this.#docsWithLocalMembershipChanges.delete(documentId);
          continue;
        }
        const currentIds = new Set(
          (await doc.cgkaMembers()).map((id) => uint8ArrayToHex(id.toBytes()))
        );
        const grantedByUs = await this.#individualsWeGrantedAccessTo(
          doc,
          ourIdHex
        );
        const baseline =
          this.#seenMembers.get(documentId) ??
          // Treat everyone we did not grant access to as already
          // known. A restart nudges once for our own adds and not at all
          // for anyone else's.
          new Set([...currentIds].filter((idHex) => !grantedByUs.has(idHex)));
        let addedByUs = 0;
        for (const idHex of currentIds) {
          if (idHex === ourIdHex) continue; // never nudge for our own membership
          if (baseline.has(idHex)) continue; // not new
          if (grantedByUs.has(idHex)) addedByUs++;
        }
        // Record the new membership regardless, so a given add nudges once.
        this.#seenMembers.set(documentId, currentIds);
        // We no longer need to track this as a local change.
        this.#docsWithLocalMembershipChanges.delete(documentId);
        if (addedByUs === 0) continue;

        // Rotate (so the new member has a derivable current key), persist the
        // rotated leaf secret for any sibling instance, then write the nudge.
        const leafSecret = await this.keyhive.forcePcsUpdate(doc);
        await this.keyhiveStorage.saveLeafSecret(leafSecret);
        const handle = await repo.find(
          `automerge:${documentId}` as AutomergeUrl
        );
        handle.change((d: any) => {
          d[NUDGE_FIELD] = Date.now();
        });
      } catch (e) {
        log.error(
          `[AMRepoKeyhiveSubduction] membership nudge check failed for ${documentId}:`,
          e
        );
      }
    }
  }

  /**
   * Grant the sync server relay access to the document, so it can sync the
   * ciphertext without being able to read it. Throws on failure, including
   * when the server's identity has not synced yet (retry after sync).
   */
  async addSyncServerRelayToDoc(docUrl: AutomergeUrl): Promise<void> {
    if (isUnprotectedDoc(docUrl)) {
      throw new UnprotectedDocError("addSyncServerRelayToDoc", docUrl);
    }
    const identifier = keyhiveIdentifierFromPeerId(
      this.networkAdapter.remotePeerId
    );
    const agent = await this.keyhive.getAgent(identifier);
    if (!agent) {
      throw new Error(
        `addSyncServerRelayToDoc: sync server agent not yet known; retry after sync. remotePeerId=${this.networkAdapter.remotePeerId}`
      );
    }
    const docId = docIdFromAutomergeUrl(docUrl);
    const doc = await this.keyhive.getDocument(docId);
    if (!doc) {
      throw new Error(
        `addSyncServerRelayToDoc: document not found in keyhive for ${docUrl} (has it synced yet?)`
      );
    }
    await this.keyhive.addMember(agent, doc.toMembered(), Access.relay(), []);
    this.noteLocalMembershipChange(docUrl);
  }

  protected syncServerIdentifierHex(): string | null {
    return uint8ArrayToHex(
      keyhiveIdentifierFromPeerId(this.networkAdapter.remotePeerId).toBytes()
    );
  }
}

/**
 * Options for wrapping an additional raw network adapter.
 */
export interface WrapNetworkAdapterOptions {
  /**
   * Share only with the configured sync server. Messages from any other peer on
   * this adapter are ignored, so nothing is exchanged with them. Default false.
   */
  onlyShareWithSyncServer?: boolean;
  /**
   * Request keyhive sync from this adapter's peers on an interval.
   * Default false.
   */
  periodicallyRequestSync?: boolean;
  /** Interval for periodic sync requests in ms. Default 2000. */
  syncRequestInterval?: number;
  /** If set, batch outgoing keyhive messages on this interval in ms. */
  batchInterval?: number;
  /** Event count that triggers compaction into an archive. Default 200. */
  archiveThreshold?: number;
}
