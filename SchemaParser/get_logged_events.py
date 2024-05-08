from ccdexplorer_fundamentals.enums import NET
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.cis import (
    CIS,
    itemStatusChangedEvent,
    itemCreatedEvent,
    StandardIdentifiers,
    LoggedEvents,
)

from ccdexplorer_fundamentals.GRPCClient.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_ContractAddress,
    CCD_ContractTraceElement,
)
from ccdexplorer_fundamentals.GRPCClient.types_pb2 import VersionedModuleSource
from ccdexplorer_fundamentals.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer_fundamentals.tooter import Tooter
from rich import print
from rich.progress import track
from pymongo import ReplaceOne

# from schema_parsing_python import Schema
from pymongo import ASCENDING
from pymongo.collection import Collection

from ccdexplorer_schema_parser.Schema import Schema

block_hash = "last_final"
# module_ref = "2826a0cf9516115386fd2c8587ab2a179fdc9b40203143044de13fdacf02733e"
# instance = "<8219,0>"

module_ref = "2c484449e32d435dcaa3921b7c04b395b3b4b8b25b1ccdc3d3002322dd04edcb"
instance = "<8527,0>"


# result = mongodb.testnet[Collections.helpers].find_one(
#     {"_id": "schema_parsing_last_processed_block"}
# )
# schema_parsing_last_processed_block = result["height"]


class GetLoggedEvents:
    def source_module_refs_from_instances(self):
        # print("source_module_refs_from_instances...")
        result = self.db[Collections.instances].find({})
        for instance in list(result):
            if instance.get("v0"):
                self.contract_address_to_module_refs_cache[instance["_id"]] = {
                    "source_module_ref": instance["v0"]["source_module"],
                    "module_name": instance["v0"]["name"][5:],
                }
            else:
                self.contract_address_to_module_refs_cache[instance["_id"]] = {
                    "source_module_ref": instance["v1"]["source_module"],
                    "module_name": instance["v1"]["name"][5:],
                }

        # print("source_module_refs_from_instances...done")
        return self.contract_address_to_module_refs_cache

    def get_schema_from_source(self, module_ref: str, net: str):
        self.source_module_ref_to_schema_cache: dict
        self.grpcclient: GRPCClient
        if not self.source_module_ref_to_schema_cache.get(module_ref):
            ms: VersionedModuleSource = (
                self.grpcclient.get_module_source_original_classes(
                    module_ref, "last_final", net=NET(net)
                )
            )
            schema = Schema(ms.v1.value, 1) if ms.v1 else Schema(ms.v0.value, 0)
            self.source_module_ref_to_schema_cache[module_ref] = schema
        else:
            schema = self.source_module_ref_to_schema_cache[module_ref]
        return schema

    def formulate_tnt_logged_event(
        self,
        tx: CCD_BlockItemSummary,
        tag_: int,
        result: itemCreatedEvent | itemStatusChangedEvent,
        effect_index: int,
        contract_address: CCD_ContractAddress,
        event: str,
        event_index: int,
    ):
        if tag_ in [236, 237]:
            _id = f"{tx.block_info.height}-{contract_address.to_str()}-{result.item_id}-{event}-{effect_index}-{event_index}"
            if result:
                result_dict = result.model_dump()
            else:
                result_dict = {}

            d = {
                "_id": _id,
                "logged_event": event,
                "result": result_dict,
                "event_type": LoggedEvents(tag_).name,
                "block_height": tx.block_info.height,
                "tx_hash": tx.hash,
                "tx_index": tx.index,
                "effect_index": effect_index,
                "event_index": event_index,
                "item_id": result.item_id,
                "item_address": f"{contract_address.to_str()}-{result.item_id}",
                "contract": contract_address.to_str(),
                "date": f"{tx.block_info.slot_time:%Y-%m-%d}",
                "timestamp": tx.block_info.slot_time,
            }
            return ReplaceOne(
                {"_id": _id},
                replacement=d,
                upsert=True,
            )

        else:
            return None

    def check_cis_6(
        self,
        cis: CIS,
        cis_6_contracts: dict[bool],
        contract_address: CCD_ContractAddress,
    ):
        if contract_address.to_str() not in cis_6_contracts:
            cis_6_contracts[contract_address.to_str()] = cis.supports_standard(
                StandardIdentifiers.CIS_6
            )

        return cis_6_contracts[contract_address.to_str()]

    def get_logged_events(self):
        # print("get_logged_events...")
        cis_6_contracts: dict[bool] = {}
        self.db: dict[Collections, Collection]
        contract_address_to_module_refs_cache = (
            self.contract_address_to_module_refs_cache
        )

        result = self.db[Collections.helpers].find_one(
            {"_id": "schema_parsing_last_processed_block"}
        )
        schema_parsing_last_processed_block = result["height"]
        block_height_to_parse_start = schema_parsing_last_processed_block + 1

        result = self.db[Collections.helpers].find_one(
            {"_id": "heartbeat_last_processed_block"}
        )
        heartbeat_last_processed_block = result["height"]
        # subtract 1 to be on the safe side
        if ((heartbeat_last_processed_block - 1) - block_height_to_parse_start) > 10000:
            block_height_to_parse_end = block_height_to_parse_start + 1000
        else:
            block_height_to_parse_end = heartbeat_last_processed_block - 1

        if block_height_to_parse_end >= block_height_to_parse_start:
            pipeline = [
                {
                    "$match": {
                        "block_info.height": {
                            "$gte": block_height_to_parse_start,
                            "$lte": block_height_to_parse_end,
                        }
                    }
                },
                {
                    "$match": {
                        "$or": [
                            {
                                "account_transaction.effects.contract_update_issued": {
                                    "$exists": True
                                }
                            },
                            {
                                "account_transaction.effects.contract_initialized": {
                                    "$exists": True
                                }
                            },
                        ]
                    }
                },
            ]
            if block_height_to_parse_start != block_height_to_parse_end:
                print(
                    f"Processing blocks: {block_height_to_parse_start:,.0f}...{block_height_to_parse_end:,.0f}"
                )
            else:
                print(f"Processing block:  {block_height_to_parse_start:,.0f}")
            result = list(self.db[Collections.transactions].aggregate(pipeline))
            tx_result = [CCD_BlockItemSummary(**x) for x in result]
            if len(tx_result) > 0:
                print(f"Relevant tx(s):  {len(tx_result)}")

            logged_events = []
            for tx in tx_result:

                effects = tx.account_transaction.effects
                if tx.account_transaction.effects.contract_update_issued:
                    pass
                elif tx.account_transaction.effects.contract_initialized:
                    pass
                else:
                    break
                if tx.account_transaction.effects.contract_update_issued:
                    for effect_index, effect in enumerate(
                        effects.contract_update_issued.effects
                    ):
                        effect: CCD_ContractTraceElement
                        if effect.updated:
                            events = effect.updated.events
                            address = effect.updated.address
                        elif effect.interrupted:
                            events = effect.interrupted.events
                            address = effect.interrupted.address
                        else:
                            break

                        (
                            contract_address,
                            source_module_ref,
                            source_module_name,
                            ci,
                            supports_cis_6,
                        ) = self.test_smart_contract_for_cis6(
                            cis_6_contracts,
                            contract_address_to_module_refs_cache,
                            address,
                        )

                        if not supports_cis_6:
                            break
                        for event_index, event in enumerate(events):
                            possible_logged_event = self.process_event_for_tnt(
                                event,
                                source_module_ref,
                                source_module_name,
                                ci,
                                tx,
                                effect_index,
                                address,
                                event_index,
                                contract_address,
                            )
                            if possible_logged_event:
                                logged_events.append(possible_logged_event)

                elif tx.account_transaction.effects.contract_initialized:
                    events = tx.account_transaction.effects.contract_initialized.events
                    address = (
                        tx.account_transaction.effects.contract_initialized.address
                    )
                    (
                        contract_address,
                        source_module_ref,
                        source_module_name,
                        ci,
                        supports_cis_6,
                    ) = self.test_smart_contract_for_cis6(
                        cis_6_contracts, contract_address_to_module_refs_cache, address
                    )

                    if not supports_cis_6:
                        break
                    for event_index, event in enumerate(events):
                        possible_logged_event = self.process_event_for_tnt(
                            event,
                            source_module_ref,
                            source_module_name,
                            ci,
                            tx,
                            effect_index,
                            address,
                            event_index,
                            contract_address,
                        )
                        if possible_logged_event:
                            logged_events.append(possible_logged_event)

            if len(logged_events) > 0:
                print(f"Logged events: {len(logged_events)}")
                _ = self.db[Collections.tnt_logged_events].bulk_write(logged_events)

            self.log_parsed_block(block_height_to_parse_end)

    def log_parsed_block(self, height: int):
        query = {"_id": "schema_parsing_last_processed_block"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "schema_parsing_last_processed_block",
                "height": height,
            },
            upsert=True,
        )

    def test_smart_contract_for_cis6(
        self,
        cis_6_contracts: dict[bool],
        contract_address_to_module_refs_cache: dict,
        address: CCD_ContractAddress,
    ):
        contract_address = address.to_str()

        source_module_ref = contract_address_to_module_refs_cache.get(contract_address)[
            "source_module_ref"
        ]
        source_module_name = contract_address_to_module_refs_cache.get(
            contract_address
        )["module_name"]
        ci = CIS(
            self.grpcclient,
            address.index,
            address.subindex,
            f"{source_module_name}.supports",
            NET(self.net),
        )
        supports_cis_6 = self.check_cis_6(ci, cis_6_contracts, address)

        return (
            contract_address,
            source_module_ref,
            source_module_name,
            ci,
            supports_cis_6,
        )

    def process_event_for_tnt(
        self,
        event,
        source_module_ref,
        source_module_name,
        ci: CIS,
        tx: CCD_BlockItemSummary,
        effect_index: int,
        address: CCD_ContractAddress,
        event_index: int,
        contract_address: str,
    ):
        possible_logged_event = None
        schema = self.get_schema_from_source(source_module_ref, self.net)
        try:
            tag, result = ci.process_tnt_log_event(event)
            if tag == 236:
                event_json = schema.event_to_json(
                    source_module_name,
                    bytes.fromhex(event),
                )

                result: itemStatusChangedEvent
                result.new_status = list(
                    event_json["ItemStatusChanged"][0]["new_status"].keys()
                )[0]
                possible_logged_event = self.formulate_tnt_logged_event(
                    tx,
                    tag,
                    result,
                    effect_index,
                    address,
                    event,
                    event_index,
                )

                # print(f"{tx.hash}: {contract_address} | {result}")
            elif tag == 237:
                event_json = schema.event_to_json(
                    source_module_name,
                    bytes.fromhex(event),
                )

                result: itemCreatedEvent
                result.initial_status = list(
                    event_json["ItemCreated"][0]["initial_status"].keys()
                )[0]
                possible_logged_event = self.formulate_tnt_logged_event(
                    tx,
                    tag,
                    result,
                    effect_index,
                    address,
                    event,
                    event_index,
                )
                # print(f"{tx.hash}: {contract_address} | {result}")
        except ValueError:
            pass

        return possible_logged_event
