import {
  AutomergeUrl,
  Heads,
  NetworkAdapter,
  PeerId,
  Repo,
} from "@automerge/automerge-repo/slim";
import { hexToUint8Array } from "../utilities.js";
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
import { SyncServer } from "../sync-server.js";
import { Active } from "./active.js";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";
import { KeyhiveEventEmitter } from "./emitter.js";
import { docIdFromAutomergeUrl, KeyhiveStorage, receiveContactCard } from "./keyhive.js";
import { signData } from "../network-adapter/messages.js";

// TODO: This is temporarily for calculating "best access". Move this and
// the best access method to WASM API.
const accessLevels: Record<string, number> = {
  None: 0,
  Relay: 1,
  Read: 2,
  Edit: 3,
  Admin: 4,
};

export class AutomergeRepoKeyhive {
  constructor(
    public readonly active: Active,
    public readonly keyhive: Keyhive,
    public readonly keyhiveStorage: KeyhiveStorage,
    public readonly peerId: PeerId,
    public readonly syncServer: SyncServer,
    public readonly networkAdapter: KeyhiveNetworkAdapter,
    public readonly emitter: KeyhiveEventEmitter,
    public readonly idFactory: (heads: Heads) => Promise<Uint8Array>,
    public readonly createKeyhiveNetworkAdapter: (networkAdapter: NetworkAdapter, onlyShareWithHardcodedServerPeerId: boolean, periodicallyRequestSync: boolean, syncRequestInterval: number, batchInterval?: number, archiveThreshold?: number) => KeyhiveNetworkAdapter,
  ) {}

  // Configure `AutomergeRepoKeyhive` to notify the provided `Repo` about
  // potential `Keyhive` membership updates. Debounces ingest-remote events
  // so that bursts of keyhive ops don't trigger sweeps on every single event.
  linkRepo(repo: Repo, options?: { debounceMs?: number, onBeforeShareConfigChanged?: () => void }) {
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

  async receiveContactCard(contactCard: ContactCard
  ): Promise<Individual | undefined> {
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

  async revokeMemberFromDoc(
    docUrl: AutomergeUrl,
    hexId: string
  ) {
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

    const membered = doc.toMembered();
    await this.keyhive.revokeMember(agent, true, membered);
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

  async generateDoc(): Promise<KeyhiveDocument> {
    return generateDoc(this.keyhive);
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

  async signData(
    data: Uint8Array,
    contactCard?: ContactCard
  ): Promise<Uint8Array> {
    return signData(this.keyhive, data, contactCard);
  }

  keyhiveIdFactory(): (heads: Heads) => Promise<Uint8Array> {
    return keyhiveIdFactory(this.networkAdapter, this.keyhive)
  }

  async stats(): Promise<Stats> {
    return await this.keyhive.stats()
  }
};

async function generateDoc(kh: Keyhive): Promise<KeyhiveDocument> {
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
