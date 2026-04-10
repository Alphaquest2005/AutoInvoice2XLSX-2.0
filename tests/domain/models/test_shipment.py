"""TDD tests for shipment domain models."""

from __future__ import annotations

from decimal import Decimal

import pytest

from autoinvoice.domain.models.shipment import BLMetadata, Consignee, Shipment


class TestConsignee:
    def test_create_with_required_fields(self) -> None:
        c = Consignee(name="Auto Brokerage Ltd")
        assert c.name == "Auto Brokerage Ltd"

    def test_defaults(self) -> None:
        c = Consignee(name="X")
        assert c.code == ""
        assert c.address == ""

    def test_frozen_immutability(self) -> None:
        c = Consignee(name="X")
        with pytest.raises(AttributeError):
            c.name = "Y"  # type: ignore[misc]


class TestBLMetadata:
    def test_create_with_required_fields(self) -> None:
        consignee = Consignee(name="Broker Co")
        bl = BLMetadata(bl_number="TSCW18489131", consignee=consignee)
        assert bl.bl_number == "TSCW18489131"
        assert bl.consignee.name == "Broker Co"

    def test_defaults(self) -> None:
        bl = BLMetadata(bl_number="X", consignee=Consignee(name="Y"))
        assert bl.shipper_names == ()
        assert bl.invoice_refs == ()
        assert bl.total_packages == 0
        assert bl.total_weight == Decimal("0")
        assert bl.vessel_name == ""
        assert bl.voyage_number == ""
        assert bl.port_of_loading == ""
        assert bl.port_of_discharge == ""

    def test_frozen_immutability(self) -> None:
        bl = BLMetadata(bl_number="X", consignee=Consignee(name="Y"))
        with pytest.raises(AttributeError):
            bl.bl_number = "Z"  # type: ignore[misc]

    def test_with_full_data(self) -> None:
        bl = BLMetadata(
            bl_number="BL-001",
            consignee=Consignee(name="ABC", code="ABC01", address="123 Main St"),
            shipper_names=("Shein US", "Amazon"),
            invoice_refs=("INV-001", "INV-002"),
            total_packages=42,
            total_weight=Decimal("1500.50"),
            vessel_name="MSC Oscar",
            port_of_loading="Shanghai",
            port_of_discharge="Kingston",
        )
        assert len(bl.shipper_names) == 2
        assert len(bl.invoice_refs) == 2
        assert bl.total_weight == Decimal("1500.50")


class TestShipment:
    def test_create_with_required_fields(self) -> None:
        bl = BLMetadata(bl_number="BL-1", consignee=Consignee(name="C"))
        shipment = Shipment(bl=bl, invoice_files=("inv1.pdf", "inv2.pdf"))
        assert shipment.bl.bl_number == "BL-1"
        assert len(shipment.invoice_files) == 2

    def test_defaults(self) -> None:
        bl = BLMetadata(bl_number="X", consignee=Consignee(name="Y"))
        shipment = Shipment(bl=bl, invoice_files=())
        assert shipment.output_dir == ""

    def test_frozen_immutability(self) -> None:
        bl = BLMetadata(bl_number="X", consignee=Consignee(name="Y"))
        shipment = Shipment(bl=bl, invoice_files=())
        with pytest.raises(AttributeError):
            shipment.output_dir = "/tmp"  # type: ignore[misc]
