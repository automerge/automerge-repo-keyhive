import { PeerId } from "@automerge/automerge-repo/slim";
import { ContactCard, Individual, Keyhive } from "@keyhive/keyhive/slim";

export type SyncServer = {
  individualId: Uint8Array;
  contactCard: ContactCard;
  contactCardJson: string;
  peerId: PeerId;
};

export async function syncServerFromContactCard(
  contactCardJson: string,
  serverPeerId: PeerId,
  keyhive: Keyhive
): Promise<SyncServer> {
  console.debug("[AMRepoKeyhive] syncServerFromContactCard: parsing server contact card");
  const serverContactCard = ContactCard.fromJson(contactCardJson);
  const serverIndividual: Individual =
    await keyhive.receiveContactCard(serverContactCard);

  const individualId = serverIndividual.id.toBytes();

  return {
    individualId,
    contactCard: serverContactCard,
    contactCardJson: contactCardJson,
    peerId: serverPeerId,
  };
}
