import { registerCommand } from "../../command";
import type { CommandEntry } from "../types";
import { accessManifestCommand } from "./access-manifest";
import { connectCommand } from "./connect";
import { disconnectCommand } from "./disconnect";
import { enrollmentAddonCommand } from "./enrollment-addon";
import { listCommand } from "./list";

export const hubCommand: CommandEntry = {
  path: "hub",
  aliases: ["hubs"],
  description: "Manage connected OCM hubs",
};

export const hubCommands: CommandEntry[] = [
  registerCommand(accessManifestCommand),
  registerCommand(connectCommand),
  registerCommand(listCommand),
  registerCommand(enrollmentAddonCommand),
  registerCommand(disconnectCommand),
];
