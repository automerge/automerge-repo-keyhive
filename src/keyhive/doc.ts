import {
  Access,
  ChangeId,
  ContactCard,
  Document as KeyhiveDocument,
  DocumentId as KeyhiveDocumentId,
  Identifier,
  Keyhive,
} from "@keyhive/keyhive/slim";
import { docIdFromAutomergeUrl } from "./keyhive.js";
import { hexToUint8Array } from "../utilities.js";
import { AutomergeUrl, Heads } from "@automerge/automerge-repo/slim";
import { KeyhiveNetworkAdapter } from "../network-adapter/network-adapter.js";

export async function generateDoc(kh: Keyhive): Promise<KeyhiveDocument> {
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

export function keyhiveIdFactory(adapter: KeyhiveNetworkAdapter, keyhive: Keyhive): (heads: Heads) => Promise<Uint8Array> {
  return async (_heads: Heads) => {
    const doc = await generateDoc(keyhive);
    return doc.doc_id.toBytes();
  };
}

export async function addMemberToDoc(
  kh: Keyhive,
  docUrl: AutomergeUrl,
  contactCard: ContactCard,
  access: Access
) {
  const agent = contactCard.toAgent();
  if (!access || !agent) {
    console.error("[AMRepoKeyhive] Failed to add member: invalid access or agent!");
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
  const doc = await kh.getDocument(docId);
  if (!doc) {
    console.error(`Failed to add member: doc not found for id ${docId}`);
    return;
  }
  await kh.addMember(agent, doc.toMembered(), access, []);
}

export async function revokeMemberFromDoc(
  kh: Keyhive,
  docUrl: AutomergeUrl,
  hexId: string
) {
  const identifier = new Identifier(hexToUint8Array(hexId));
  const agent = await kh.getAgent(identifier);

  if (!agent) {
    console.error("[AMRepoKeyhive] Agent to revoke not found");
    return;
  }

  const docId = docIdFromAutomergeUrl(docUrl);
  const doc = await kh.getDocument(docId);
  if (!doc) {
    console.error(`Failed to revoke member: doc not found for id ${docId}`);
    return;
  }

  const membered = doc.toMembered();
  await kh.revokeMember(agent, true, membered);
}
