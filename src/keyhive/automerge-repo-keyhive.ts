import {
  AutomergeUrl,
  Heads,
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
import { encodeKeyhiveMessageData } from "../network-adapter/messages.js";

export const KEYHIVE_DB_KEY = "keyhive-db";
export const KEYHIVE_ARCHIVES_KEY = "/archives/";
export const KEYHIVE_EVENTS_KEY = "/ops/";

export class AutomergeRepoKeyhive {
  constructor(
    public active: Active,
    public keyhive: Keyhive,
    public keyhiveStorage: KeyhiveStorage,
    public peerId: PeerId,
    public syncServer: SyncServer,
    public networkAdapter: KeyhiveNetworkAdapter,
    public emitter: KeyhiveEventEmitter,
    public idFactory: (heads: Heads) => Promise<Uint8Array>
  ) {}

  // Configure `AutomergeRepoKeyhive` to notify the provided `Repo` about
  // `Keyhive` membership updates.
  linkRepo(repo: Repo) {
    this.emitter.on("update", async (event: KeyhiveEvent) => {
      if (event.variant === "DELEGATED" || event.variant === "REVOKED") {
        repo.shareConfigChanged()
      }
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
    if (!docId) {
      console.error(`Failed to parse docId from AutomergeUrl`);
      return;
    }
    const doc = await this.keyhive.getDocument(docId);
    if (!doc) {
      console.error(`Failed to add member: doc not found for id ${docId}`);
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
      console.error(`Failed to revoke member: doc not found for id ${docId}`);
      return;
    }

    const membered = doc.toMembered();
    await this.keyhive.revokeMember(agent, true, membered);
  }

  async generateDoc(): Promise<KeyhiveDocument> {
    return generateDoc(this.keyhive)
  }

  async accessForDoc(id: Identifier, doc_id: KeyhiveDocumentId): Promise<Access | undefined> {
    return await this.keyhive.accessForDoc(id, doc_id)
  }

  async docMemberCapabilities(doc_id: KeyhiveDocumentId): Promise<Membership[]> {
    return await this.keyhive.docMemberCapabilities(doc_id)
  }

  async signData(
    data: Uint8Array,
    contactCard?: ContactCard
  ): Promise<Uint8Array> {
    try {
      const signed = await this.keyhive.trySign(data);
      return encodeKeyhiveMessageData({
        contactCard,
        signed,
      });
    } catch (error) {
      console.error("[AMRepoKeyhive] Error during signing:", error);
      throw error;
    }
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
  const changeIdArray = Uint8Array.from({ length: 10 }, () =>
    Math.floor(Math.random() * 256)
  );
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
