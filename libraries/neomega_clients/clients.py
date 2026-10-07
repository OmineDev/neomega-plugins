"""Typed asynchronous clients over the public Host service receipt protocol."""
from datetime import datetime, timedelta, timezone
import uuid
import time
from typing import Any, cast
from neomega_runtime.services import ServiceFailed, ServiceRejected
from .models import *


class Client:
    plugin_id: str

    def __init__(self, services, *, timeout: float = 5):
        if not 0 < timeout <= 30:
            raise ValueError('service timeout must be in (0, 30]')
        self.services = services
        self.timeout = timeout

    def prepare(self, method: str, arguments: dict[str, Any], *, idempotency_key: str, deadline: str):
        """Persist intent.payload() before submission for durable consumer workflows."""
        return self.services.prepare(f'plugin.{self.plugin_id}.{method}', arguments,
            major=1, idempotency_key=idempotency_key, deadline=deadline)

    async def invoke(self, method: str, arguments: dict[str, Any]) -> Any:
        """One admission only. Every exception retains the exact intent; never replay."""
        intent = self.prepare(method, arguments, idempotency_key=uuid.uuid4().hex,
            deadline=(datetime.now(timezone.utc) + timedelta(seconds=self.timeout)).isoformat())
        call_id = None
        try:
            call_id = await self.services.submit(intent, timeout=self.timeout)
            receipt = await self.services.wait(call_id, timeout=self.timeout)
            if receipt['state'] != 'replied':
                if receipt.get('rejection'):
                    raise ServiceRejected(receipt['rejection']['code'], receipt=receipt)
                raise ServiceFailed(receipt)
            return receipt['result']
        except BaseException as exc:
            exc.intent = intent
            if call_id is not None:
                exc.call_id = call_id
            raise

    async def receipt(self, call_id: str) -> dict[str, Any]:
        """Read a known Host call receipt; this never submits a new call."""
        return await self.services.get(call_id, timeout=self.timeout)


class InteractionClient(Client):
    plugin_id = "neomega.interaction"

    async def register(self, arguments: InteractionRequest) -> MenuResult:
        return cast(MenuResult, await self.invoke("register", dict(arguments)))

    async def open(self, arguments: InteractionRequest) -> MenuResult:
        return cast(MenuResult, await self.invoke("open", dict(arguments)))

    async def respond(self, arguments: InteractionRequest) -> MenuResult:
        return cast(MenuResult, await self.invoke("respond", dict(arguments)))

    async def cancel(self, arguments: InteractionRequest) -> MenuResult:
        return cast(MenuResult, await self.invoke("cancel", dict(arguments)))

    async def status(self, arguments: InteractionRequest) -> MenuResult:
        return cast(MenuResult, await self.invoke("status", dict(arguments)))

    async def unregister(self, arguments: InteractionRequest) -> MenuResult:
        return cast(MenuResult, await self.invoke("unregister", dict(arguments)))


class PlayersClient(Client):
    plugin_id = "neomega.players"

    async def resolve(self, arguments: PlayerRequest) -> PlayerResult:
        return cast(PlayerResult, await self.invoke("resolve", dict(arguments)))

    async def history(self, arguments: PlayerRequest) -> PlayerResult:
        return cast(PlayerResult, await self.invoke("history", dict(arguments)))

    async def get(self, arguments: PlayerRequest) -> PlayerResult:
        return cast(PlayerResult, await self.invoke("get", dict(arguments)))

    async def patch(self, arguments: PlayerRequest) -> PlayerResult:
        return cast(PlayerResult, await self.invoke("patch", dict(arguments)))

    async def query(self, arguments: PlayerRequest) -> PlayerResult:
        return cast(PlayerResult, await self.invoke("query", dict(arguments)))

    async def export(self, arguments: PlayerRequest) -> PlayerResult:
        return cast(PlayerResult, await self.invoke("export", dict(arguments)))

    async def register_schema(self, arguments: PlayerRequest) -> PlayerSchemaSaved:
        return cast(PlayerSchemaSaved, await self.invoke("register_schema", dict(arguments)))

    async def get_schema(self, arguments: PlayerRequest) -> PlayerSchemaResult:
        return cast(PlayerSchemaResult, await self.invoke("get_schema", dict(arguments)))

    async def import_fields(self, arguments: PlayerRequest) -> PlayerResult:
        """Import this installation's exported fields; player identity is never rewritten."""
        return cast(PlayerResult, await self.invoke("import", dict(arguments)))


class ObservationsClient(Client):
    plugin_id = "neomega.observations"

    async def subscribe(self, arguments: ObservationRequest) -> ObservationResult:
        return cast(ObservationResult, await self.invoke("subscribe", dict(arguments)))

    async def snapshot(self, arguments: ObservationRequest) -> ObservationResult:
        return cast(ObservationResult, await self.invoke("snapshot", dict(arguments)))

    async def refresh(self, arguments: ObservationRequest) -> ObservationResult:
        return cast(ObservationResult, await self.invoke("refresh", dict(arguments)))

    async def status(self, arguments: ObservationRequest) -> ObservationResult:
        return cast(ObservationResult, await self.invoke("status", dict(arguments)))

    async def poll(self, arguments: ObservationRequest) -> ObservationResult:
        return cast(ObservationResult, await self.invoke("poll", dict(arguments)))

    async def unsubscribe(self, arguments: ObservationRequest) -> ObservationResult:
        return cast(ObservationResult, await self.invoke("unsubscribe", dict(arguments)))

    async def read(self, arguments: ObservationRequest) -> ObservationPage:
        return cast(ObservationPage, await self.invoke("read", dict(arguments)))


class CbBridgeClient(Client):
    plugin_id = "neomega.cbbridge"

    async def register(self, arguments: BridgeRequest) -> BridgeRoute:
        return cast(BridgeRoute, await self.invoke("register", dict(arguments)))

    async def unregister(self, arguments: BridgeRequest) -> BridgeRemoved:
        return cast(BridgeRemoved, await self.invoke("unregister", dict(arguments)))

    async def subscribe(self, arguments: BridgeRequest) -> BridgeSubscription:
        return cast(BridgeSubscription, await self.invoke("subscribe", dict(arguments)))

    async def poll(self, arguments: BridgeRequest) -> BridgePoll:
        return cast(BridgePoll, await self.invoke("poll", dict(arguments)))

    async def status(self, arguments: BridgeRequest) -> BridgeStatus:
        return cast(BridgeStatus, await self.invoke("status", dict(arguments)))


class ChunksClient(Client):
    plugin_id = "neomega.chunks"

    async def request(self, arguments: ChunkRequest) -> ChunkResult:
        return cast(ChunkResult, await self.invoke("request", dict(arguments)))

    async def read(self, arguments: ChunkRequest) -> ChunkResult:
        return cast(ChunkResult, await self.invoke("read", dict(arguments)))

    async def subscribe(self, arguments: ChunkRequest) -> ChunkResult:
        return cast(ChunkResult, await self.invoke("subscribe", dict(arguments)))

    async def cancel(self, arguments: ChunkRequest) -> ChunkResult:
        return cast(ChunkResult, await self.invoke("cancel", dict(arguments)))

    async def poll(self, arguments: ChunkRequest) -> ChunkResult:
        return cast(ChunkResult, await self.invoke("poll", dict(arguments)))


class WorldToolsClient(Client):
    plugin_id = "neomega.worldtools"

    async def inspect(self, arguments: WorldRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("inspect", dict(arguments)))

    async def prepare_task(self, arguments: WorldRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("prepare", dict(arguments)))

    async def execute(self, arguments: WorldMutationRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("execute", dict(arguments)))

    async def status(self, arguments: WorldRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("status", dict(arguments)))

    async def cancel(self, arguments: WorldMutationRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("cancel", dict(arguments)))

    async def resume(self, arguments: WorldMutationRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("resume", dict(arguments)))

    async def snapshot(self, arguments: WorldRequest) -> SnapshotPage:
        return cast(SnapshotPage, await self.invoke("snapshot", dict(arguments)))

    async def fatalder_prepare(self, arguments: WorldRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("fatalder_prepare", dict(arguments)))

    async def fatalder_status(self, arguments: WorldRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("fatalder_status", dict(arguments)))

    async def fatalder_confirm(self, arguments: WorldRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("fatalder_confirm", dict(arguments)))

    async def fatalder_pause(self, arguments: WorldControlRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("fatalder_pause", dict(arguments)))

    async def fatalder_resume(self, arguments: WorldControlRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("fatalder_resume", dict(arguments)))

    async def fatalder_cancel(self, arguments: WorldControlRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("fatalder_cancel", dict(arguments)))

    async def fatalder_recover(self, arguments: WorldRequest) -> WorldResult:
        return cast(WorldResult, await self.invoke("fatalder_recover", dict(arguments)))


class MusicClient(Client):
    plugin_id = "neomega.music"

    async def play(self, arguments: MusicRequest) -> MusicResult:
        return cast(MusicResult, await self.invoke("play", dict(arguments)))

    async def status(self, arguments: MusicRequest) -> MusicResult:
        return cast(MusicResult, await self.invoke("status", dict(arguments)))

    async def stop(self, arguments: MusicRequest) -> MusicResult:
        return cast(MusicResult, await self.invoke("stop", dict(arguments)))


class ScoresClient(Client):
    plugin_id = "neomega.scores"

    async def objective(self, arguments: ScoreRequest) -> ScoreResult:
        return cast(ScoreResult, await self.invoke("objective", dict(arguments)))

    async def mutate(self, arguments: ScoreRequest) -> ScoreResult:
        return cast(ScoreResult, await self.invoke("mutate", dict(arguments)))

    async def query(self, arguments: ScoreRequest) -> ScoreResult:
        return cast(ScoreResult, await self.invoke("query", dict(arguments)))

    async def rank(self, arguments: ScoreRequest) -> ScoreResult:
        return cast(ScoreResult, await self.invoke("rank", dict(arguments)))

    async def sync(self, arguments: ScoreRequest) -> ScoreResult:
        return cast(ScoreResult, await self.invoke("sync", dict(arguments)))


class RolesClient(Client):
    plugin_id = "neomega.roles"

    async def define(self, arguments: RoleRequest) -> RoleResult:
        return cast(RoleResult, await self.invoke("define", dict(arguments)))

    async def assign(self, arguments: RoleRequest) -> RoleResult:
        return cast(RoleResult, await self.invoke("assign", dict(arguments)))

    async def revoke(self, arguments: RoleRequest) -> RoleResult:
        return cast(RoleResult, await self.invoke("revoke", dict(arguments)))

    async def check(self, arguments: RoleRequest) -> RoleResult:
        return cast(RoleResult, await self.invoke("check", dict(arguments)))

    async def list(self, arguments: RoleRequest) -> RoleResult:
        return cast(RoleResult, await self.invoke("list", dict(arguments)))


class ModerationClient(Client):
    plugin_id = "neomega.moderation"

    async def ban(self, arguments: ModerationRequest) -> ModerationResult:
        return cast(ModerationResult, await self.invoke("ban", dict(arguments)))

    async def unban(self, arguments: ModerationRequest) -> ModerationResult:
        return cast(ModerationResult, await self.invoke("unban", dict(arguments)))

    async def check(self, arguments: ModerationRequest) -> ModerationResult:
        return cast(ModerationResult, await self.invoke("check", dict(arguments)))

    async def list(self, arguments: ModerationRequest) -> ModerationResult:
        return cast(ModerationResult, await self.invoke("list", dict(arguments)))


class EconomyClient(Client):
    plugin_id = "neomega.economy"

    async def balance(self, arguments: EconomyRequest) -> Balance:
        return cast(Balance, await self.invoke("balance", dict(arguments)))

    async def post(self, arguments: EconomyRequest) -> Balance:
        return cast(Balance, await self.invoke("post", dict(arguments)))

    async def transfer(self, arguments: EconomyRequest) -> Balance:
        return cast(Balance, await self.invoke("transfer", dict(arguments)))

    async def hold(self, arguments: EconomyRequest) -> Balance:
        return cast(Balance, await self.invoke("hold", dict(arguments)))

    async def release(self, arguments: EconomyRequest) -> Balance:
        return cast(Balance, await self.invoke("release", dict(arguments)))

    async def ledger(self, arguments: EconomyRequest) -> LedgerResult:
        return cast(LedgerResult, await self.invoke("ledger", dict(arguments)))

    async def receipt_request(self, arguments: EconomyRequest) -> BusinessReceipt:
        """Read a durable provider request result after uncertain admission."""
        return cast(BusinessReceipt, await self.invoke("receipt", dict(arguments)))


class SchedulerClient(Client):
    plugin_id = "neomega.scheduler"

    async def schedule(self, arguments: SchedulerRequest) -> SchedulerResult:
        return cast(SchedulerResult, await self.invoke("schedule", dict(arguments)))

    async def cancel(self, arguments: SchedulerRequest) -> SchedulerResult:
        return cast(SchedulerResult, await self.invoke("cancel", dict(arguments)))

    async def query(self, arguments: SchedulerRequest) -> SchedulerResult:
        return cast(SchedulerResult, await self.invoke("query", dict(arguments)))

    async def claim(self, arguments: SchedulerRequest) -> SchedulerResult:
        return cast(SchedulerResult, await self.invoke("claim", dict(arguments)))

    async def ack(self, arguments: SchedulerRequest) -> SchedulerResult:
        return cast(SchedulerResult, await self.invoke("ack", dict(arguments)))


class MessagingClient(Client):
    plugin_id = "neomega.messaging"

    @staticmethod
    def new_request_id() -> str:
        """Create once per business intent; persist before send and retain on uncertainty."""
        return f"v2:{time.time_ns() // 1_000_000}:{uuid.uuid4().hex}"


    async def send(self, arguments: MessagingRequest) -> MessagingResult:
        return cast(MessagingResult, await self.invoke("send", dict(arguments)))

    async def routes(self, arguments: MessagingRequest) -> MessagingResult:
        return cast(MessagingResult, await self.invoke("routes", dict(arguments)))

    async def status(self, arguments: MessagingRequest) -> MessagingResult:
        return cast(MessagingResult, await self.invoke("status", dict(arguments)))

    async def subscribe(self, arguments: MessagingRequest) -> MessagingResult:
        return cast(MessagingResult, await self.invoke("subscribe", dict(arguments)))
