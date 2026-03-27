from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ElectionRequest(_message.Message):
    __slots__ = ("sender_id",)
    SENDER_ID_FIELD_NUMBER: _ClassVar[int]
    sender_id: int
    def __init__(self, sender_id: _Optional[int] = ...) -> None: ...

class ElectionResponse(_message.Message):
    __slots__ = ("alive", "responder_id")
    ALIVE_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_ID_FIELD_NUMBER: _ClassVar[int]
    alive: bool
    responder_id: int
    def __init__(self, alive: bool = ..., responder_id: _Optional[int] = ...) -> None: ...

class CoordinatorRequest(_message.Message):
    __slots__ = ("leader_id",)
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    leader_id: int
    def __init__(self, leader_id: _Optional[int] = ...) -> None: ...

class CoordinatorResponse(_message.Message):
    __slots__ = ("acknowledged",)
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    acknowledged: bool
    def __init__(self, acknowledged: bool = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("sender_id",)
    SENDER_ID_FIELD_NUMBER: _ClassVar[int]
    sender_id: int
    def __init__(self, sender_id: _Optional[int] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("alive", "leader_id")
    ALIVE_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    alive: bool
    leader_id: int
    def __init__(self, alive: bool = ..., leader_id: _Optional[int] = ...) -> None: ...
