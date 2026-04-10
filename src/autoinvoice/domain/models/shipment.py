"""Shipment domain models - BL and consignee data."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Consignee:
    """Shipment consignee."""

    name: str
    code: str = ""
    address: str = ""


@dataclass(frozen=True)
class BLMetadata:
    """Bill of Lading metadata."""

    bl_number: str
    consignee: Consignee
    shipper_names: tuple[str, ...] = ()
    invoice_refs: tuple[str, ...] = ()
    total_packages: int = 0
    total_weight: Decimal = Decimal("0")
    vessel_name: str = ""
    voyage_number: str = ""
    port_of_loading: str = ""
    port_of_discharge: str = ""


@dataclass(frozen=True)
class Shipment:
    """Complete shipment with BL and invoices."""

    bl: BLMetadata
    invoice_files: tuple[str, ...]
    output_dir: str = ""
