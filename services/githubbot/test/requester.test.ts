import { describe, expect, test } from "bun:test";
import {
  githubLifecycleRequesterMetadata,
  githubRequesterRequirements,
} from "../src/session-api";
import type { GithubbotApiAuthor, GithubbotApiMessage } from "../src/types";

function author(userId: string, userName = "octocat"): GithubbotApiAuthor {
  return {
    fullName: "Octo Cat",
    isBot: false,
    isMe: false,
    userId,
    userName,
  };
}

describe("githubRequesterRequirements", () => {
  test("requires GitHub and Linear OAuth for a native GitHub requester", () => {
    expect(githubRequesterRequirements(author(" 12345 "))).toEqual({
      requester_credentials_required: true,
      requester_required_providers: ["github", "linear"],
      requester_origin: "github_user",
    });
  });

  test("does not treat lifecycle messages as human requesters", () => {
    for (const id of ["github-review", "github-pr-manager", "github-issue", "0", ""]) {
      expect(githubRequesterRequirements(author(id, id))).toEqual({});
    }
  });
});

describe("githubLifecycleRequesterMetadata", () => {
  test("binds a Linear-originated management turn to the app principal", () => {
    const message: GithubbotApiMessage = {
      attachments: [],
      author: author("github-pr-manager", "github-pr-manager"),
      id: "review-1",
      isMention: true,
      raw: {
        githubbotManagement: true,
        requesterPrincipalForeignId: "linearbot-agent",
        linearIssueIdentifier: "ENG-123",
      },
      text: "Address review",
      threadId: "github-manage:acme/repo:7",
      timestamp: new Date(0).toISOString(),
    };
    expect(githubLifecycleRequesterMetadata(message)).toEqual({
      requester_principal_foreign_id: "linearbot-agent",
      requester_credentials_required: true,
      requester_required_providers: ["linear"],
      requester_origin: "linear_app",
      linear_issue_identifier: "ENG-123",
    });
  });

  test("rejects an app assertion on a human or non-management message", () => {
    const message: GithubbotApiMessage = {
      attachments: [],
      author: author("12345"),
      id: "comment-1",
      isMention: true,
      raw: { requesterPrincipalForeignId: "linearbot-agent" },
      text: "hello",
      threadId: "github:acme/repo:7",
      timestamp: new Date(0).toISOString(),
    };
    expect(githubLifecycleRequesterMetadata(message)).toEqual({});
  });
});
