#!/usr/bin/env python3
"""
ASYCUDA XML Parser - Parse ASYCUDA World XML files to extract classifications and tax rates.

This module extracts item-level classification data from ASYCUDA customs declaration XMLs,
which can be used to update/correct classifications in the system.
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import json
import re


@dataclass
class TaxLine:
    """Represents a single taxation line for an item."""
    tax_code: Optional[str] = None
    tax_base: Optional[float] = None
    tax_rate: Optional[float] = None
    tax_amount: Optional[float] = None
    mode_of_payment: Optional[str] = None
    calculation_type: Optional[str] = None


@dataclass
class AsycudaItem:
    """Represents a single item from an ASYCUDA declaration."""
    item_number: int
    commodity_code: str  # 8-digit HS code
    precision_1: Optional[str] = None  # Additional precision suffix
    precision_4: Optional[str] = None  # SKU/item reference
    description_of_goods: Optional[str] = None  # Official HS description
    commercial_description: Optional[str] = None  # Product description
    country_of_origin: Optional[str] = None
    extended_procedure: Optional[str] = None
    national_procedure: Optional[str] = None
    quantity: Optional[float] = None
    unit_code: Optional[str] = None
    gross_weight: Optional[float] = None
    net_weight: Optional[float] = None
    cif_value: Optional[float] = None
    invoice_value: Optional[float] = None
    statistical_value: Optional[float] = None
    taxes: List[TaxLine] = None

    def __post_init__(self):
        if self.taxes is None:
            self.taxes = []

    @property
    def full_tariff_code(self) -> str:
        """Returns the full tariff code including precision."""
        if self.precision_1:
            return f"{self.commodity_code}{self.precision_1}"
        return self.commodity_code

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        d['full_tariff_code'] = self.full_tariff_code
        return d


@dataclass
class AsycudaDeclaration:
    """Represents a complete ASYCUDA customs declaration."""
    file_path: str
    declaration_type: str  # IM (Import), EX (Export), etc.
    office_code: str
    office_name: str
    registration_number: Optional[str] = None
    registration_date: Optional[str] = None
    declarant_code: Optional[str] = None
    declarant_name: Optional[str] = None
    exporter_code: Optional[str] = None
    exporter_name: Optional[str] = None
    consignee_code: Optional[str] = None
    consignee_name: Optional[str] = None
    destination_country: Optional[str] = None
    origin_country: Optional[str] = None
    total_items: int = 0
    total_cif: Optional[float] = None
    total_invoice: Optional[float] = None
    currency_code: Optional[str] = None
    items: List[AsycudaItem] = None

    def __post_init__(self):
        if self.items is None:
            self.items = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = {
            'file_path': self.file_path,
            'declaration_type': self.declaration_type,
            'office_code': self.office_code,
            'office_name': self.office_name,
            'registration_number': self.registration_number,
            'registration_date': self.registration_date,
            'declarant_code': self.declarant_code,
            'declarant_name': self.declarant_name,
            'exporter_code': self.exporter_code,
            'exporter_name': self.exporter_name,
            'consignee_code': self.consignee_code,
            'consignee_name': self.consignee_name,
            'destination_country': self.destination_country,
            'origin_country': self.origin_country,
            'total_items': self.total_items,
            'total_cif': self.total_cif,
            'total_invoice': self.total_invoice,
            'currency_code': self.currency_code,
            'items': [item.to_dict() for item in self.items],
        }
        return d


def get_text(element: Optional[ET.Element], default: str = None) -> Optional[str]:
    """Safely get text content from an element."""
    if element is None:
        return default
    text = element.text
    if text is None or text.strip() == '' or text.strip().lower() == 'null':
        # Check for <null /> child
        null_child = element.find('null')
        if null_child is not None:
            return default
        return default
    return text.strip()


def get_float(element: Optional[ET.Element], default: float = None) -> Optional[float]:
    """Safely get float value from an element."""
    text = get_text(element)
    if text is None:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def parse_taxation_line(tax_elem: ET.Element) -> Optional[TaxLine]:
    """Parse a single taxation line element."""
    tax_code = get_text(tax_elem.find('Duty_tax_code'))
    if tax_code is None:
        return None

    return TaxLine(
        tax_code=tax_code,
        tax_base=get_float(tax_elem.find('Duty_tax_Base')),
        tax_rate=get_float(tax_elem.find('Duty_tax_rate')),
        tax_amount=get_float(tax_elem.find('Duty_tax_amount')),
        mode_of_payment=get_text(tax_elem.find('Duty_tax_MP')),
        calculation_type=get_text(tax_elem.find('Duty_tax_Type_of_calculation')),
    )


def parse_item(item_elem: ET.Element, item_number: int) -> Optional[AsycudaItem]:
    """Parse a single Item element from the declaration."""
    # Get tarification/HS code
    tarification = item_elem.find('Tarification')
    if tarification is None:
        return None

    hs_code = tarification.find('HScode')
    if hs_code is None:
        return None

    commodity_code = get_text(hs_code.find('Commodity_code'))
    if commodity_code is None:
        return None

    # Get goods description
    goods_desc = item_elem.find('Goods_description')

    # Get supplementary unit quantity
    supp_unit = tarification.find('Supplementary_unit')
    quantity = None
    unit_code = None
    if supp_unit is not None:
        quantity = get_float(supp_unit.find('Suppplementary_unit_quantity'))
        unit_code = get_text(supp_unit.find('Suppplementary_unit_code'))

    # Get valuation
    valuation = item_elem.find('Valuation_item')
    gross_weight = None
    net_weight = None
    cif_value = None
    invoice_value = None
    statistical_value = None

    if valuation is not None:
        weight = valuation.find('Weight_itm')
        if weight is not None:
            gross_weight = get_float(weight.find('Gross_weight_itm'))
            net_weight = get_float(weight.find('Net_weight_itm'))

        cif_value = get_float(valuation.find('Total_CIF_itm'))
        statistical_value = get_float(valuation.find('Statistical_value'))

        item_invoice = valuation.find('Item_Invoice')
        if item_invoice is not None:
            invoice_value = get_float(item_invoice.find('Amount_national_currency'))

    # Parse taxation lines
    taxes = []
    taxation = item_elem.find('Taxation')
    if taxation is not None:
        for tax_line in taxation.findall('Taxation_line'):
            tax = parse_taxation_line(tax_line)
            if tax is not None:
                taxes.append(tax)

    return AsycudaItem(
        item_number=item_number,
        commodity_code=commodity_code,
        precision_1=get_text(hs_code.find('Precision_1')),
        precision_4=get_text(hs_code.find('Precision_4')),
        description_of_goods=get_text(goods_desc.find('Description_of_goods')) if goods_desc else None,
        commercial_description=get_text(goods_desc.find('Commercial_Description')) if goods_desc else None,
        country_of_origin=get_text(goods_desc.find('Country_of_origin_code')) if goods_desc else None,
        extended_procedure=get_text(tarification.find('Extended_customs_procedure')),
        national_procedure=get_text(tarification.find('National_customs_procedure')),
        quantity=quantity,
        unit_code=unit_code,
        gross_weight=gross_weight,
        net_weight=net_weight,
        cif_value=cif_value,
        invoice_value=invoice_value,
        statistical_value=statistical_value,
        taxes=taxes,
    )


def parse_asycuda_xml(file_path: str) -> AsycudaDeclaration:
    """
    Parse an ASYCUDA World XML file and extract all classification data.

    Args:
        file_path: Path to the ASYCUDA XML file

    Returns:
        AsycudaDeclaration object containing all extracted data
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Get identification info
    identification = root.find('Identification')
    office = identification.find('Office_segment') if identification else None
    type_elem = identification.find('Type') if identification else None
    registration = identification.find('Registration') if identification else None

    # Get property info
    property_elem = root.find('Property')
    nbers = property_elem.find('Nbers') if property_elem else None

    # Get traders info
    traders = root.find('Traders')
    exporter = traders.find('Exporter') if traders else None
    consignee = traders.find('Consignee') if traders else None

    # Get declarant info
    declarant = root.find('Declarant')

    # Get general info
    general = root.find('General_information')
    country = general.find('Country') if general else None
    destination = country.find('Destination') if country else None
    export = country.find('Export') if country else None

    # Get valuation totals
    valuation = root.find('Valuation')
    gs_invoice = valuation.find('Gs_Invoice') if valuation else None

    # Parse all items
    items = []
    item_number = 1
    for item_elem in root.findall('Item'):
        item = parse_item(item_elem, item_number)
        if item is not None:
            items.append(item)
            item_number += 1

    return AsycudaDeclaration(
        file_path=file_path,
        declaration_type=get_text(type_elem.find('Type_of_declaration')) if type_elem else None,
        office_code=get_text(office.find('Customs_clearance_office_code')) if office else None,
        office_name=get_text(office.find('Customs_Clearance_office_name')) if office else None,
        registration_number=get_text(registration.find('Number')) if registration else None,
        registration_date=get_text(registration.find('Date')) if registration else None,
        declarant_code=get_text(declarant.find('Declarant_code')) if declarant else None,
        declarant_name=get_text(declarant.find('Declarant_name')) if declarant else None,
        exporter_code=get_text(exporter.find('Exporter_code')) if exporter else None,
        exporter_name=get_text(exporter.find('Exporter_name')) if exporter else None,
        consignee_code=get_text(consignee.find('Consignee_code')) if consignee else None,
        consignee_name=get_text(consignee.find('Consignee_name')) if consignee else None,
        destination_country=get_text(destination.find('Destination_country_code')) if destination else None,
        origin_country=get_text(country.find('Country_of_origin_name')) if country else None,
        total_items=int(get_text(nbers.find('Total_number_of_items'), '0')) if nbers else len(items),
        total_cif=get_float(valuation.find('Total_CIF')) if valuation else None,
        total_invoice=get_float(gs_invoice.find('Amount_national_currency')) if gs_invoice else None,
        currency_code=get_text(gs_invoice.find('Currency_code')) if gs_invoice else None,
        items=items,
    )


def parse_multiple_xmls(file_paths: List[str]) -> List[AsycudaDeclaration]:
    """Parse multiple ASYCUDA XML files."""
    declarations = []
    for fp in file_paths:
        try:
            decl = parse_asycuda_xml(fp)
            declarations.append(decl)
        except Exception as e:
            print(f"Error parsing {fp}: {e}")
    return declarations


def extract_classifications(declaration: AsycudaDeclaration) -> List[Dict[str, Any]]:
    """
    Extract classification records from a declaration for database update.

    Returns a list of classification records with the tariff code, description,
    and any reference info that can be used to match against existing classifications.
    """
    classifications = []
    for item in declaration.items:
        classification = {
            'tariff_code': item.full_tariff_code,
            'commodity_code': item.commodity_code,
            'precision': item.precision_1,
            'sku_reference': item.precision_4,
            'description': item.description_of_goods,
            'commercial_description': item.commercial_description,
            'country_of_origin': item.country_of_origin,
            'source': 'asycuda_xml',
            'source_file': declaration.file_path,
            'declaration_type': declaration.declaration_type,
            'registration_number': declaration.registration_number,
            'registration_date': declaration.registration_date,
        }

        # Add tax rates if available
        if item.taxes:
            classification['taxes'] = [
                {
                    'code': t.tax_code,
                    'rate': t.tax_rate,
                    'amount': t.tax_amount,
                }
                for t in item.taxes
            ]

        classifications.append(classification)

    return classifications


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Parse ASYCUDA XML files')
    parser.add_argument('files', nargs='+', help='XML files to parse')
    parser.add_argument('--output', '-o', help='Output JSON file')
    parser.add_argument('--classifications', '-c', action='store_true',
                        help='Output classifications only')

    args = parser.parse_args()

    declarations = parse_multiple_xmls(args.files)

    if args.classifications:
        all_classifications = []
        for decl in declarations:
            all_classifications.extend(extract_classifications(decl))
        output = all_classifications
    else:
        output = [d.to_dict() for d in declarations]

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Output written to {args.output}")
    else:
        print(json.dumps(output, indent=2))
