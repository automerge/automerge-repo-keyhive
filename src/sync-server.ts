import { log } from "./logging.js";
import { PeerId } from "@automerge/automerge-repo/slim";
import { ContactCard, Keyhive } from "@keyhive/keyhive/slim";
import { KeyhiveStorage, receiveContactCard } from "./keyhive/keyhive.js";

export type SyncServer = {
  individualId: Uint8Array;
  contactCard: ContactCard;
  peerId: PeerId;
};

export async function syncServerFromContactCard(
  contactCardBytes: Uint8Array,
  serverPeerId: PeerId,
  keyhive: Keyhive,
  keyhiveStorage: KeyhiveStorage
): Promise<SyncServer> {
  log.debug(
    "[AMRepoKeyhive] syncServerFromContactCard: parsing server contact card"
  );
  const serverContactCard = ContactCard.fromBytes(contactCardBytes);
  const serverIndividual = await receiveContactCard(
    keyhive,
    serverContactCard,
    keyhiveStorage
  );
  if (!serverIndividual) {
    throw Error(
      `Invalid server contact card: ${serverContactCard.id.toString()}`
    );
  }

  const individualId = serverIndividual.id.toBytes();

  return {
    individualId,
    contactCard: serverContactCard,
    peerId: serverPeerId,
  };
}
