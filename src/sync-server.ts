import { PeerId } from "@automerge/automerge-repo/slim";
import { ContactCard, Keyhive } from "@keyhive/keyhive/slim";
import { KeyhiveStorage, receiveContactCard } from "./keyhive/keyhive.js";

export type SyncServer = {
  individualId: Uint8Array;
  contactCard: ContactCard;
  peerId: PeerId;
};

export async function syncServerFromContactCard(
  contactCardJson: string,
  serverPeerId: PeerId,
  keyhive: Keyhive,
  keyhiveStorage: KeyhiveStorage
): Promise<SyncServer> {
  console.debug(
    "[AMRepoKeyhive] syncServerFromContactCard: parsing server contact card"
  );
  const serverContactCard = ContactCard.fromJson(contactCardJson);
  const serverIndividual = await receiveContactCard(
    keyhive,
    serverContactCard,
    keyhiveStorage
  );
  if (!serverIndividual) {
    throw Error(`Invalid server contact card: ${contactCardJson}`);
  }

  const individualId = serverIndividual.id.toBytes();

  return {
    individualId,
    contactCard: serverContactCard,
    peerId: serverPeerId,
  };
}
