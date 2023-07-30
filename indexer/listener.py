from apibara.starknet import EventFilter, Filter, StarkNetIndexer, felt
from starknet_py.contract import ContractFunction
from apibara.indexer import Info
from apibara.starknet.cursor import starknet_cursor
from apibara.protocol.proto.stream_pb2 import DataFinality
from apibara.indexer.indexer import IndexerConfiguration
from apibara.starknet.proto.starknet_pb2 import Block
from apibara.starknet.proto.types_pb2 import FieldElement
from typing import List
from utils import decode_felt_to_domain_string


class Listener(StarkNetIndexer):
    def __init__(self, conf, logger) -> None:
        super().__init__()
        self.conf = conf
        self.logger = logger
        self.handle_pending_data = self.handle_data
        self.last_amount = 0
        self.last_buyer = 0
        self.auto_renew = False
        self.sponsor_comm = 0
        self.sponsor_addr = 0

    def indexer_id(self) -> str:
        return self.conf.indexer_id

    def initial_configuration(self) -> Filter:
        filter = Filter().with_header(weak=True)
        self.event_map = dict()

        def add_filter(contract, event):
            selector = ContractFunction.get_selector(event)
            self.event_map[selector] = event
            filter.add_event(
                EventFilter()
                .with_from_address(felt.from_int(contract))
                .with_keys([felt.from_int(selector)])
            )

        add_filter(self.conf.referral_contract, "on_commission")
        add_filter(self.conf.renewal_contract, "domain_renewed")
        add_filter(self.conf.eth_contract, "Transfer")
        add_filter(self.conf.naming_contract, "starknet_id_update")

        return IndexerConfiguration(
            filter=filter,
            starting_cursor=starknet_cursor(self.conf.starting_block),
            finality=DataFinality.DATA_STATUS_ACCEPTED,
        )

    async def handle_data(self, info: Info, block: Block):
        # Handle one block of data
        for event_with_tx in block.events:
            tx_hash = felt.to_hex(event_with_tx.transaction.meta.hash)
            # print(tx_hash)
            event = event_with_tx.event
            event_key = felt.to_int(event.keys[0])

            if not event_key in self.event_map:
                print(self.event_map)
                print(event_key)
                continue

            event_name = self.event_map[event_key]
            if event_name == "on_commission":
                await self.on_referral(info, block, event.from_address, event.data)
            if event_name == "domain_renewed":
                await self.on_auto_renew(info, block, event.from_address, event.data)
            if event_name == "Transfer":
                await self.on_funds_sent(info, block, event.from_address, event.data)
            elif event_name == "starknet_id_update":
                await self.on_starknet_id_update(
                    info, block, event.from_address, event.data
                )

    async def on_referral(
        self, info: Info, block: Block, contract: FieldElement, data: List[FieldElement]
    ) -> int:
        self.sponsor_comm = felt.to_int(data[1]) + felt.to_int(data[2]) * 2**128
        self.sponsor_addr = felt.to_int(data[3])

    async def on_auto_renew(
        self, info: Info, block: Block, contract: FieldElement, data: List[FieldElement]
    ) -> int:
        self.auto_renew = True

    async def on_funds_sent(
        self, info: Info, block: Block, contract: FieldElement, data: List[FieldElement]
    ) -> int:
        to = felt.to_int(data[1])
        if to == self.conf.naming_contract:
            self.last_buyer = felt.to_int(data[0])
            self.last_amount = felt.to_int(data[2]) + felt.to_int(data[3]) * 2**128

    async def on_starknet_id_update(
        self, info: Info, block: Block, contract: FieldElement, data: List[FieldElement]
    ):
        arr_len = felt.to_int(data[0])
        if arr_len != 1:
            return
        domain = ""
        for i in range(arr_len):
            domain += decode_felt_to_domain_string(felt.to_int(data[1 + i])) + "."
        if domain:
            domain += "stark"
        owner = str(felt.to_int(data[arr_len + 1]))
        expiry = felt.to_int(data[arr_len + 2])

        # we want to upsert
        existing = await info.storage.find_one_and_update(
            "domains",
            {"domain": domain, "_chain.valid_to": None},
            {
                "$set": {
                    "domain": domain,
                    "expiry": expiry,
                }
            },
        )

        sale_type = None
        if existing is None:
            sale_type = "purchase"
            await info.storage.insert_one(
                "domains",
                {
                    "domain": domain,
                    "expiry": expiry,
                    "creation_date": block.header.timestamp.ToDatetime(),
                },
            )

            duration = expiry - block.header.timestamp.ToSeconds()

        else:
            sale_type = "renewal"
            duration = expiry - existing["expiry"]

        await info.storage.insert_one(
            "sales",
            {
                "domain": domain,
                "type": sale_type,
                "price": self.last_amount,
                "timestamp": block.header.timestamp.ToSeconds(),
                "duration": duration,
                "auto": self.auto_renew,
                "sponsor": self.sponsor_addr,
                "sponsor_comm": self.sponsor_comm,
            },
        )
        self.auto_renew = False
        self.sponsor_comm = 0
        self.sponsor_addr = 0
        self.last_amount = 0

        self.logger.info(
            f"{sale_type}: {domain}, {duration/86400}d for {self.last_amount / 1e18} eth"
        )
