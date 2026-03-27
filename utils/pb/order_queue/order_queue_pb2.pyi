from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrderItem(_message.Message):
    __slots__ = ("name", "quantity")
    NAME_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    name: str
    quantity: int
    def __init__(self, name: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class EnqueueRequest(_message.Message):
    __slots__ = ("order_id", "purchaser_name", "purchaser_email", "credit_card_number", "credit_card_expiration", "credit_card_cvv", "billing_street", "billing_city", "billing_state", "billing_zip", "billing_country", "items", "suggested_books")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    PURCHASER_NAME_FIELD_NUMBER: _ClassVar[int]
    PURCHASER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    CREDIT_CARD_NUMBER_FIELD_NUMBER: _ClassVar[int]
    CREDIT_CARD_EXPIRATION_FIELD_NUMBER: _ClassVar[int]
    CREDIT_CARD_CVV_FIELD_NUMBER: _ClassVar[int]
    BILLING_STREET_FIELD_NUMBER: _ClassVar[int]
    BILLING_CITY_FIELD_NUMBER: _ClassVar[int]
    BILLING_STATE_FIELD_NUMBER: _ClassVar[int]
    BILLING_ZIP_FIELD_NUMBER: _ClassVar[int]
    BILLING_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    SUGGESTED_BOOKS_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    purchaser_name: str
    purchaser_email: str
    credit_card_number: str
    credit_card_expiration: str
    credit_card_cvv: str
    billing_street: str
    billing_city: str
    billing_state: str
    billing_zip: str
    billing_country: str
    items: _containers.RepeatedCompositeFieldContainer[OrderItem]
    suggested_books: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, order_id: _Optional[str] = ..., purchaser_name: _Optional[str] = ..., purchaser_email: _Optional[str] = ..., credit_card_number: _Optional[str] = ..., credit_card_expiration: _Optional[str] = ..., credit_card_cvv: _Optional[str] = ..., billing_street: _Optional[str] = ..., billing_city: _Optional[str] = ..., billing_state: _Optional[str] = ..., billing_zip: _Optional[str] = ..., billing_country: _Optional[str] = ..., items: _Optional[_Iterable[_Union[OrderItem, _Mapping]]] = ..., suggested_books: _Optional[_Iterable[str]] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("ok",)
    OK_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    def __init__(self, ok: bool = ...) -> None: ...

class DequeueRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DequeueResponse(_message.Message):
    __slots__ = ("ok", "order_id", "purchaser_name", "purchaser_email", "credit_card_number", "credit_card_expiration", "credit_card_cvv", "billing_street", "billing_city", "billing_state", "billing_zip", "billing_country", "items", "suggested_books")
    OK_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    PURCHASER_NAME_FIELD_NUMBER: _ClassVar[int]
    PURCHASER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    CREDIT_CARD_NUMBER_FIELD_NUMBER: _ClassVar[int]
    CREDIT_CARD_EXPIRATION_FIELD_NUMBER: _ClassVar[int]
    CREDIT_CARD_CVV_FIELD_NUMBER: _ClassVar[int]
    BILLING_STREET_FIELD_NUMBER: _ClassVar[int]
    BILLING_CITY_FIELD_NUMBER: _ClassVar[int]
    BILLING_STATE_FIELD_NUMBER: _ClassVar[int]
    BILLING_ZIP_FIELD_NUMBER: _ClassVar[int]
    BILLING_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    SUGGESTED_BOOKS_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    order_id: str
    purchaser_name: str
    purchaser_email: str
    credit_card_number: str
    credit_card_expiration: str
    credit_card_cvv: str
    billing_street: str
    billing_city: str
    billing_state: str
    billing_zip: str
    billing_country: str
    items: _containers.RepeatedCompositeFieldContainer[OrderItem]
    suggested_books: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, ok: bool = ..., order_id: _Optional[str] = ..., purchaser_name: _Optional[str] = ..., purchaser_email: _Optional[str] = ..., credit_card_number: _Optional[str] = ..., credit_card_expiration: _Optional[str] = ..., credit_card_cvv: _Optional[str] = ..., billing_street: _Optional[str] = ..., billing_city: _Optional[str] = ..., billing_state: _Optional[str] = ..., billing_zip: _Optional[str] = ..., billing_country: _Optional[str] = ..., items: _Optional[_Iterable[_Union[OrderItem, _Mapping]]] = ..., suggested_books: _Optional[_Iterable[str]] = ...) -> None: ...
