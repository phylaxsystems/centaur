import { describe, expect, test } from "bun:test";
import { linearAppRequesterMetadata } from "../src/session-api";

describe("linearAppRequesterMetadata", () => {
  test("binds delegated issue work to the isolated Linear app principal", () => {
    expect(linearAppRequesterMetadata("linear:issue-uuid:s:agent-session")).toEqual({
      requester_principal_foreign_id: "linearbot-agent",
      requester_credentials_required: true,
      requester_required_providers: ["linear"],
      requester_origin: "linear_app",
      linear_issue_id: "issue-uuid",
    });
  });
});
