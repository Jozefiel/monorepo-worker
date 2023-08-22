# from typing import Optional
# from typing import Any
# from enum import Enum
#
# from frinx.common.conductor_enums import TaskResultStatus
# from frinx.common.worker.service import ServiceWorkersImpl
# from frinx.common.worker.task_def import TaskDefinition
# from frinx.common.worker.task_def import TaskInput
# from frinx.common.worker.task_def import TaskOutput
# from frinx.common.worker.task_def import TaskExecutionProperties
#
# from frinx.common.worker.task_result import TaskResult
# from frinx.common.worker.worker import WorkerImpl
#
# from frinx.services.inventory.model import *
#
# from frinx.common.graphql.client import GraphqlClient
# from frinx.common.frinx_rest import INVENTORY_HEADERS, INVENTORY_URL_BASE
#
# client = GraphqlClient(INVENTORY_URL_BASE, headers=INVENTORY_HEADERS)
#
#
# class PaginationCursorType(str, Enum):
#     AFTER = 'after'
#     BEFORE = 'before'
#
#
# class InventoryDevice(ServiceWorkersImpl):
#     # class GetDevices(WorkerImpl):
#     #     DEVICE: DeviceConnection = DeviceConnection(
#     #         edges=DeviceEdge(
#     #             cursor=True,
#     #             node=Device(
#     #                 name=True,
#     #                 id=True,
#     #                 isInstalled=True,
#     #                 zone=Zone(
#     #                     name=True,
#     #                     id=True
#     #                 )
#     #             )
#     #         )
#     #     )
#     #
#     #     DevicesQuery(
#     #         payload=DEVICE,
#     #         after='after',
#     #         before='before',
#     #         last=1,
#     #         first=1,
#     #         filter=FilterDevicesInput(
#     #             labels=['label'],
#     #             deviceName='deviceName'
#     #         ),
#     #         orderBy=DeviceOrderByInput(
#     #             sortKey=SortDeviceBy.NAME,
#     #             direction=SortDirection.ASC
#     #         )
#     #     )
#     #
#     #     class WorkerDefinition(TaskDefinition):
#     #         name: str = 'INVENTORY_get_devices'
#     #         description: str = 'Get devices from inventory'
#     #
#     #     class WorkerInput(TaskInput):
#     #         labels: Optional[list[str]]
#     #         device_name: Optional[str]
#     #         size: Optional[int]
#     #         cursor: Optional[str]
#     #         type: Optional[PaginationCursorType]
#     #         sort: Optional[SortDeviceBy]
#     #         direction: Optional[SortDirection]
#     #
#     #     class WorkerOutput(TaskOutput):
#     #         response: dict[str, Any]
#     #
#     #     def execute(self, worker_input: WorkerInput) -> TaskResult:
#     #         query = DevicesQuery(
#     #             payload=self.DEVICE
#     #         )
#     #
#     #         filters = FilterDevicesInput()
#     #         if worker_input.device_name:
#     #             filters.device_name = worker_input.device_name
#     #         if worker_input.labels:
#     #             filters.labels = worker_input.labels
#     #         if filters.dict(exclude_none=True):
#     #             query.filter = filters
#     #
#     #         if worker_input.sort or worker_input.direction:
#     #             orders = DeviceOrderByInput(
#     #                 direction=SortDirection(worker_input.direction),
#     #                 sortKey=SortDeviceBy(worker_input.sort)
#     #             )
#     #             query.order_by = orders
#     #
#     #         match worker_input.type:
#     #             case PaginationCursorType.AFTER:
#     #                 query.first = worker_input.size
#     #                 query.after = worker_input.cursor
#     #             case PaginationCursorType.BEFORE:
#     #                 query.last = worker_input.size
#     #                 query.before = worker_input.cursor
#     #
#     #         response = client.execute(query=query.render(), variables={})
#     #         return TaskResult(status=TaskResultStatus.COMPLETED, output={'response': response})
#     #
#     # class GetBlueprints(WorkerImpl):
#     #     BLUEPRINT: BlueprintConnection = BlueprintConnection(
#     #         edges=BlueprintEdge(
#     #             cursor=True,
#     #             node=Blueprint(
#     #                 name=True,
#     #                 id=True,
#     #                 template=True
#     #             )
#     #         )
#     #     )
#     #
#     #     BlueprintsQuery(
#     #         payload=BLUEPRINT,
#     #         after='after',
#     #         before='before',
#     #         last=1,
#     #         first=1,
#     #     )
#     #
#     #     class WorkerDefinition(TaskDefinition):
#     #         name: str = 'INVENTORY_get_blueprints'
#     #         description: str = 'Get device to inventory mutation'
#     #
#     #     class WorkerInput(TaskInput):
#     #         size: Optional[int]
#     #         cursor: Optional[str]
#     #         type: Optional[PaginationCursorType]
#     #
#     #     class WorkerOutput(TaskOutput):
#     #         response: dict[str, Any]
#     #
#     #     def execute(self, worker_input: WorkerInput) -> TaskResult:
#     #         query = BlueprintsQuery(
#     #             payload=self.BLUEPRINT
#     #         )
#     #
#     #         match worker_input.type:
#     #             case PaginationCursorType.AFTER:
#     #                 query.first = worker_input.size
#     #                 query.after = worker_input.cursor
#     #             case PaginationCursorType.BEFORE:
#     #                 query.last = worker_input.size
#     #                 query.before = worker_input.cursor
#     #
#     #         response = client.execute(query=query.render(), variables={})
#     #         return TaskResult(status=TaskResultStatus.COMPLETED, output={'response': response})
#     #
#     # class DeleteDevice(WorkerImpl):
#     #     DEVICE: DeleteDevicePayload = DeleteDevicePayload(
#     #         device=Device(
#     #             name=True,
#     #             id=True,
#     #             isInstalled=True,
#     #             zone=Zone(
#     #                 name=True,
#     #                 id=True
#     #             )
#     #         )
#     #     )
#     #
#     #     DeleteDeviceMutation(
#     #         payload=DEVICE,
#     #         id='id'
#     #     )
#     #
#     #     class WorkerDefinition(TaskDefinition):
#     #         name: str = 'INVENTORY_delete_device'
#     #         description: str = 'Delete device by id from inventory'
#     #
#     #     class WorkerInput(TaskInput):
#     #         id: str
#     #
#     #     class WorkerOutput(TaskOutput):
#     #         response: dict[str, Any]
#     #
#     #     def execute(self, worker_input: WorkerInput) -> TaskResult:
#     #
#     #         mutation = DeleteDeviceMutation(
#     #             payload=self.DEVICE,
#     #             id=worker_input.id
#     #         )
#     #
#     #         response = client.execute(query=mutation.render(), variables={})
#     #         return TaskResult(status=TaskResultStatus.COMPLETED, output={'response': response})
#
#     class AddDevice(WorkerImpl):
#
#         exclude_empty_inputs = True
#
#         DEVICE: AddDevicePayload = AddDevicePayload(
#             device=Device(
#                 name=True,
#                 id=True,
#                 isInstalled=True,
#                 zone=Zone(
#                     name=True,
#                     id=True
#                 )
#             )
#         )
#
#         AddDeviceMutation(
#             payload=DEVICE,
#             input=AddDeviceInput(
#                 name='name',
#                 zoneId='name',
#                 labelIds=['label_ids'],
#                 deviceSize=DeviceSize.SMALL,
#                 serviceState=DeviceServiceState.PLANNING,
#                 mountParameters='',
#                 blueprintId='',
#                 model='',
#                 vendor='',
#                 address='',
#                 username='',
#                 password='',
#                 port=22,
#                 deviceType='',
#                 version=''
#             )
#         )
#
#         class ExecutionProperties(TaskExecutionProperties):
#             exclude_empty_inputs = True
#             transform_string_to_json_valid = True
#
#         class WorkerDefinition(TaskDefinition):
#             name: str = 'INVENTORY_add_device'
#             description: str = 'Add device to inventory'
#
#         class WorkerInput(TaskInput):
#             name: str
#             zoneId: str
#             labelIds: list[str]
#             deviceSize: Optional[DeviceSize]
#             serviceState: Optional[DeviceServiceState]
#             mountParameters: Optional[str]
#             blueprintId: Optional[str]
#             model: Optional[str]
#             vendor: Optional[str]
#             address: Optional[str]
#             username: Optional[str]
#             password: Optional[str]
#             port: Optional[int]
#             deviceType: Optional[str]
#             version: dict[str, Any]
#
#         class WorkerOutput(TaskOutput):
#             response: dict[str, Any]
#
#         def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
#
#             print(worker_input)
#             mutation = AddDeviceMutation(
#                 payload=self.DEVICE,
#                 input=AddDeviceInput(**worker_input.dict(exclude_none=True,)),
#             )
#
#             print(mutation.render())
#             response = client.execute(query=mutation.render(), variables={})
#             return TaskResult(status=TaskResultStatus.COMPLETED, output=self.WorkerOutput(response=response))
