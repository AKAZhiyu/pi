// Admission for hosts that must not start runs: `whenIdle: "reject"` (#11149).
import { type AssistantMessage, fauxAssistantMessage, fauxText, fauxToolCall, Type } from "@earendil-works/pi-ai";
import { type Conversation, ConversationIdle, defineTool, InboxDoc, LiveDoc } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import { chatSetup, openChat, unanswered } from "./chat-support.ts";
import { addTool } from "./harness-support.ts";
import { ControlledStorage, context } from "./session-support.ts";
import { deferred } from "./task-support.ts";

const MODEL = { provider: "faux", modelId: "faux-1" } as const;

const failure = (): AssistantMessage =>
	fauxAssistantMessage([], { stopReason: "error", errorMessage: "Invalid request" });

/** Open a chat whose run hangs until aborted, and wait until it works on its first input. */
async function openBusy(storage = new ControlledStorage()) {
	const setup = chatSetup();
	const busy = unanswered();
	setup.faux.setResponses([busy.step]);
	const opened = await openChat(storage, setup);
	const first = await opened.root.submit({ type: "input", content: "work" }, context);
	await busy.reached;
	return { ...opened, storage, first };
}

describe("whenIdle reject", () => {
	// #11149
	it("rejects input to an idle conversation without writing", async () => {
		const storage = new ControlledStorage();
		const { harness, root } = await openChat(storage, chatSetup());
		const commits = storage.commits.length;
		const rejected = root.submit({ type: "input", content: "hi", whenBusy: "steer", whenIdle: "reject" }, context);
		await expect(rejected).rejects.toBeInstanceOf(ConversationIdle);
		await expect(rejected).rejects.toMatchObject({ conversationId: root.id });
		expect(storage.commits.length).toBe(commits);
		expect((await harness.snapshot(LiveDoc, root.id, context))?.run).toBeUndefined();
		await harness.close(context);
	});

	// #11149
	it("rejects input to an idle conversation that keeps queued items after a failed run", async () => {
		const setup = chatSetup();
		const failing = deferred();
		const requested = deferred();
		setup.faux.setResponses([
			async () => {
				requested.resolve();
				await failing.promise;
				return failure();
			},
		]);
		const { harness, root } = await openChat(new ControlledStorage(), setup);
		const first = await root.submit({ type: "input", content: "first" }, context);
		await requested.promise;
		const queued = await root.submit({ type: "input", content: "queued" }, context);
		failing.resolve();
		expect(await first.wait(context)).toMatchObject({ status: "unanswered", reason: "model_error" });
		await harness.waitForIdle(context);
		const rejected = root.submit({ type: "input", content: "steer", whenBusy: "steer", whenIdle: "reject" }, context);
		await expect(rejected).rejects.toBeInstanceOf(ConversationIdle);
		expect(await queued.status(context)).toMatchObject({ status: "queued" });
		expect((await harness.snapshot(LiveDoc, root.id, context))?.run).toBeUndefined();
		await harness.close(context);
	});

	// #11149
	it("steers a working run", async () => {
		const { harness, root, first } = await openBusy();
		const steer = await root.submit(
			{ type: "input", content: "also this", whenBusy: "steer", whenIdle: "reject" },
			context,
		);
		expect(await steer.status(context)).toMatchObject({ type: "input", status: "queued" });
		const inbox = await harness.snapshot(InboxDoc, root.id, context);
		expect(inbox?.items).toEqual([{ id: steer.id, mode: "steer", content: "also this" }]);
		expect((await harness.snapshot(LiveDoc, root.id, context))?.run?.inputs).toEqual([first.id]);
		await harness.close(context);
	});

	// #11149
	it("returns the submission of a known request ID, also once idle", async () => {
		const setup = chatSetup();
		setup.faux.setResponses([fauxAssistantMessage([fauxText("done")])]);
		const { harness, root } = await openChat(new ControlledStorage(), setup);
		const first = await root.submit({ type: "input", content: "work", requestId: "r1" }, context);
		await first.wait(context);
		const draft = { type: "input", content: "work", requestId: "r1", whenBusy: "steer", whenIdle: "reject" } as const;
		expect((await root.submit(draft, context)).id).toBe(first.id);
		await harness.close(context);
	});

	// #11149
	it("rejects through a tool's conversation handle", async () => {
		const setup = chatSetup();
		let other: Conversation | undefined;
		let rejection: unknown;
		addTool(
			setup.registry,
			defineTool({
				name: "nudge",
				description: "Steers another conversation",
				parameters: Type.Object({}),
				execute: async (_args, api, callContext) => {
					const handle = (await api.conversation(other!.id, callContext))!;
					const draft = { type: "input", content: "nudge", whenBusy: "steer", whenIdle: "reject" } as const;
					rejection = await handle.submit(draft, callContext).catch((error: unknown) => error);
					return { output: [{ type: "text", text: "nudged" }] };
				},
			}),
		);
		setup.faux.setResponses([
			fauxAssistantMessage([fauxToolCall("nudge", {}, { id: "c1" })], { stopReason: "toolUse" }),
			fauxAssistantMessage([fauxText("ok")]),
		]);
		const { harness, root } = await openChat(new ControlledStorage(), setup);
		other = await harness.createConversation({ ownership: { kind: "ownerless" }, agent: { model: MODEL } }, context);
		await (await root.submit({ type: "input", content: "go" }, context)).wait(context);
		expect(rejection).toBeInstanceOf(ConversationIdle);
		expect((await harness.snapshot(LiveDoc, other.id, context))?.run).toBeUndefined();
		await harness.close(context);
	});
});
