import { PeerId } from "@automerge/automerge-repo/slim";
import {
  ContactCard,
  Individual,
  Keyhive,
} from "@keyhive/keyhive/slim";

export type SyncServer = {
  individualId: Uint8Array;
  contactCard: string;
  peerId: PeerId;
};

export async function syncServerFromContactCard(
  contactCardJson: string,
  serverPeerId: PeerId,
  keyhive: Keyhive
): Promise<SyncServer> {
  const serverContactCard = ContactCard.fromJson(contactCardJson);
  const serverIndividual: Individual =
    await keyhive.receiveContactCard(serverContactCard);

  const individualId = serverIndividual.id.toBytes();

  return {
    individualId,
    contactCard: contactCardJson,
    peerId: serverPeerId,
  };
}

export async function getSyncServerIndividual(
  syncServer: SyncServer,
  keyhive: Keyhive
): Promise<Individual | null> {
  const contactCard = ContactCard.fromJson(syncServer.contactCard);
  console.debug("[Adapter] BEFORE Getting individual for server");
  // Try to get the Individual from keyhive
  const individual = await keyhive.receiveContactCard(contactCard);
  console.debug("[Adapter] AFTER Got individual for server");
  return individual;
}
