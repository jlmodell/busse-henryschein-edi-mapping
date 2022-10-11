import httpx
from rich import print
import re
from nanoid import generate
from datetime import datetime

# constants
NATIONAL_DRUG_CODE = "101419"


async def get_lot_by_lot(lot: str) -> str:
    """Get the lot by lot number and return expiration date"""
    url = f"https://lots.bhd-ny.com/?lot={lot}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        json = {}
        if response.status_code == 200:
            json = response.json()

        return json.get("expiration", None)


input_map = {
    "H": [
        "trading_partner",
        "edi_doc",
        "doc_type",
        "cust_po",
        "curr_date",
        "curr_time",
        "packslip_date",
        "packslip",
        "_9",
        "_10",
        "_11",
        "_12",
        "weight_div_by_1000",
        "_14",
        "territory_code",
    ],  # header
    "O": [
        "so",
        "shipment_number",
        "_",
        "order_date",
        "weight",
        "num_of_cases",
        "_6",
        "_7",
        "_8",
        "_9",
        "_10",
        "_11",
        "_12",
        "_13",
        "fob",
        "terms",
        "_16",
        "_17",
        "bill_to_name",
        "bill_to_account",
        "bill_to_addr",
        "bill_to_addr_2",
        "bill_to_city",
        "bill_to_state",
        "bill_to_zip",
        "bill_to_country",
        "ship_to_name",
        "ship_to_account",
        "ship_to_id",
        "ship_to_addr",
        "ship_to_addr_2",
        "ship_to_city",
        "ship_to_state",
        "ship_to_zip",
        "ship_to_country",
        "manufacturer",
        "manufacturer_addr",
        "_37",
        "manufacturer_city",
        "manufacturer_state",
        "manufacturer_zip",
        "_41",
        "_42",
        "_43",
        "_44",
        "_45",
        "_46",
        "_47",
        "_48",
        "_49",
        "_50",
        "_51",
        "_52",
        "_53",
        "_54",
        "_55",
        "_56",
        "_57",
        "_58",
        "edi_id",
        "_60",
        "_61",
        "_62",
        "ship_type",
        "_64",
        "_65",
        "_66",
        "_67",
        "_68",
        "_69",
        "_70",
        "_71",
        "_72",
        "_73",
        "_74",
        "_75",
        "_76",
        "_77",
        "_78",
        "_79",
        "_80",
        "_81",
        "_82",
        "_83",
        "_84",
        "_85",
        "_86",
        "_87",
        "_88",
        "_89",
        "_90",
        "_91",
        "_92",
        "_93",
        "_94",
        "_95",
        "_96",
        "_97",
        "_98",
        "_99",
        "_100",
        "_101",
        "_102",
        "_103",
        "_104",
        "_105",
        "_106",
        "_107",
        "_108",
        "_109",
        "_110",
        "_111",
        "_112",
        "_113",
        "_114",
        "_115",
        "_116",
        "_117",
        "_118",
        "_119",
        "_120",
    ],  # order
    "PS": [
        "packslip",
        "_2",
        "_3",
        "shipment_ref_number",
        "carrier_name",
        "carrier_code",
        "scac",
    ],  # packslip
    "I": [
        "line_number",
        "item",
        "_2",
        "_3",
        "_4",
        "cust_item",
        "_6",
        "item_description",
        "cust_po",
        "_9",
        "UoM",
        "quantity_ordered_div_by_1000",
        "quantity_shipped_div_by_1000",
        "_13",
        "_14",
        "_15",
        "unit_price_div_by_100",
        "_17",
        "_18",
        "_19",
        "_20",
        "_21",
        "_22",
        "_23",
        "_24",
        "_25",
        "rx",
        "_27",
        "_28",
        "_29",
        "_30",
        "_31",
        "_32",
        "_33",
        "_34",
        "_35",
        "_36",
        "_37",
        "_38",
        "_39",
        "_40",
        "_41",
        "_42",
        "_43",
        "_44",
        "_45",
        "_46",
        "_47",
        "_48",
        "_49",
        "_50",
        "_51",
        "_52",
        "_53",
        "_54",
        "_55",
        "_56",
        "_57",
        "_58",
        "_59",
        "_60",
    ],  # item
    "LT": ["_1"],  # lot
}


# Example 856 data

with open(r"c:\temp\m2k_856O.app_20220928123004.bak", "r") as f:
    input_example = f.read()

HS_ASN = []
start = False
order = ""

for line in input_example.splitlines():
    if line.startswith('"H"~'):
        if line.startswith('"H"~"HENRYSCHEIN"') and order == "":
            start = True
        else:
            start = False
            if order != "":
                HS_ASN.append(order)
                order = ""

    if start:
        order += line + "\n"

print(len(HS_ASN))
print(HS_ASN[0])


def check_for_key(key, value, dict) -> any:
    return dict.get(key, value)


parsed_example = {
    "H": {
        "trading_partner": "HENRYSCHEIN",
        "edi_doc": "856",
        "doc_type": "00",
        "cust_po": "525251000501",
        "curr_date": "20221007",
        "curr_time": "080759",
        "packslip_date": "20221006",
        "packslip": "525251-5",
        "weight_div_by_1000": "186000",
        "territory_code": "40",
    },
    "O": {
        "so": "525251",
        "shipment_number": "5",
        "order_date": "20220606",
        "weight": "186000",
        "num_of_cases": "40",
        "fob": "PLANT",
        "terms": "1% 15 NET 30 DAYS",
        "bill_to_name": "HENRY SCHEIN INC//GREENVILLE",
        "bill_to_account": "2091",
        "bill_to_addr": "ACCOUNTS PAYABLE DEPT",
        "bill_to_addr_2": "PO BOX 2880",
        "bill_to_city": "GREENVILLE",
        "bill_to_state": "SC",
        "bill_to_zip": "29602",
        "bill_to_country": "United States of America",
        "ship_to_name": "HENRY SCHEIN//GRAPEVINE,TX",
        "ship_to_account": "2091*5859",
        "ship_to_id": "0124308800017",
        "ship_to_addr": "1001 NOLEN DRIVE",
        "ship_to_addr_2": "SUITE 400",
        "ship_to_city": "GRAPEVINE",
        "ship_to_state": "TX",
        "ship_to_zip": "76051",
        "ship_to_country": "United States of America",
        "manufacturer": "Busse Hospital Disposables",
        "manufacturer_addr": "75 Arkay Drive",
        "manufacturer_city": "Hauppauge",
        "manufacturer_state": "NY",
        "manufacturer_zip": "11788",
        "edi_id": "0124308800017",
        "ship_type": "GRND",
    },
    "PS": {
        "packslip": "525251-5",
        "shipment_ref_number": "REF#397259",
        "scac": "EVER",
        "carrier_code": "CN",
        "carrier_name": "EVLS",
    },
    "I": [
        {
            "line_number": "2",
            "item": "7856R2",
            "item_description": "Ster 20gxS//S//Epid//Ty10CRX",
            "cust_po": "21049029",
            "UoM": "CS",
            "quantity_ordered_div_by_1000": 20.0,
            "quantity_shipped_div_by_1000": 20.0,
            "unit_price_div_by_100": 125.25,
            "lot": [],
        },
        {
            "line_number": "11",
            "item": "9998R1",
            "item_description": "Ster Epidural Tray 10//CS",
            "cust_po": "21049029",
            "UoM": "CS",
            "quantity_shipped_div_by_1000": 20.0,
            "unit_price_div_by_100": 79.0,
            "rx": False,
            "lot": [["2230809", "20", "2025-08-30"]],
        },
    ],
}


async def parse_856_input(input_data: str) -> dict:
    """Parse the 856 input data and return a dict"""
    output = {}
    item_idx = 0
    for line in input_data.splitlines():
        if line:
            line_type, *line_data = line.replace('"', "").split("~")
            if line_type in input_map:
                if line_type == "LT" or line_type == "I":
                    if line_type == "I":
                        if item_idx == 0:
                            output["I"] = []

                        item_data = dict(zip(input_map[line_type], line_data))

                        for k, v in item_data.items():
                            if k == "rx":
                                if v:
                                    item_data[k] = True
                                else:
                                    item_data[k] = False

                            if re.search(r"_div_by_", k, re.IGNORECASE):
                                if re.search(r"^unit_price", k, re.IGNORECASE):
                                    item_data[k] = int(v) / int(k.split("_div_by_")[1])
                                else:
                                    item_data[k] = int(
                                        int(v) / int(k.split("_div_by_")[1])
                                    )

                        output[line_type].append(item_data)

                    elif line_type == "LT":
                        if line_data:

                            line_data.append(None)

                            if line_data[0].startswith("Lot//Qty: "):
                                line_data[0] = line_data[0].replace("Lot//Qty: ", "")
                                line_data[0] = line_data[0].split("|")[0]

                            lot_expiration = await get_lot_by_lot(line_data[0])

                            if lot_expiration:
                                line_data.append(lot_expiration.replace("-", ""))
                            else:
                                line_data.append(None)

                            output["I"][item_idx]["lot"] = line_data

                        item_idx += 1

                elif line_type == "H":
                    item_data = dict(zip(input_map[line_type], line_data))

                    for k, v in item_data.items():
                        print(k, re.search(r"_div_by_", k, re.IGNORECASE))
                        if re.search(r"_div_by_", k, re.IGNORECASE):
                            item_data[k] = int(int(v) / int(k.split("_div_by_")[1]))

                    output[line_type] = item_data

                else:
                    output[line_type] = dict(zip(input_map[line_type], line_data))

    # drop all keys within dict within dict that start with "_"
    _output = {
        k: {k2: v2 for k2, v2 in v.items() if not k2.startswith("_")}
        for k, v in output.items()
        if k not in ["I"]
    }

    for i, j in enumerate(output["I"]):
        output["I"][i] = {k: v for k, v in j.items() if not k.startswith("_")}

    _output = {**_output, "I": output["I"]}

    return _output


# Example 856
output_example = """
ST*856*114210001
BSN*00*525251000501*20221007*080759
HL*1*0*S
TD1*CTN*40****A3*186*01
TD5**2*EVER
REF*BM*525251-5
DTM*011*20221007
N1*SF*Busse Hospital Disposables
N3*75 Arkay Drive
N4*Hauppauge*NY*11788
N1*ST*HENRY SCHEIN/GRAPEVINE,TX
N3*1001 NOLEN DRIVE
N4*GRAPEVINE*TX*76051
HL*2*1*O
PRF*21049029
HL*3*2*I
LIN*2*VC*7856R2***ND*101419*LT*2230789
SN1*2*20*CA
HL*4*2*I
LIN*11*VC*9998R1***ND*101419*LT*2230809
SN1*11*20*CA
HL*5*4*D
CTT*5
SE*24*114210001
"""

isa_gs = """
ISA*00*          *00*          *01*002418234T     *01*012430880      *221007*0829*U*00303*000011356*0*P*>
GS*SH*002418234T*012430880*20221007*0829*11421*X*004010
"""


def generate_856_from_output(output: dict) -> str:
    """Generate an 856 from the output dict"""
    H = output["H"]
    O = output["O"]
    I = output["I"]
    PS = output["PS"]
    date, time = datetime.now().strftime("%Y%m%d,%H%M%S").split(",")

    assert len(I) > 0, "No items in the output dict"
    CUST_PO = check_for_key("cust_po", "", I[0])

    isa_id = generate("1234567890", 9)
    gs_id = generate("1234567890", 9)
    st_id = generate("1234567890", 9)

    hl_tracker = 1
    inner_hl_tracker = 0

    ST = ["ST", "856", st_id]
    BSN = [
        "BSN",
        check_for_key("doc_type", "00", H),
        check_for_key("cust_po", "", H),
        date,
        time,
    ]
    HL1 = ["HL", hl_tracker, inner_hl_tracker, "S"]
    hl_tracker += 1
    inner_hl_tracker += 1
    TD1 = [
        "TD1",
        "CTN",
        check_for_key("num_of_cases", "", O),
        "",
        "",
        "",
        "A3",
        check_for_key("weight_div_by_1000", "", H),
        "01",
    ]
    TD5 = [
        "TD5",
        "",
        "2",
        check_for_key("scac", "", PS),
        "",
        check_for_key("shipment_ref_number", "", PS),
    ]
    REF = [
        "REF",
        "2I" if re.search(r"(FED|UPS)", check_for_key("scac", "", PS)) else "CN",
        check_for_key("shipment_ref_number", "", PS),
    ]
    REF_2 = ["REF", "BM", check_for_key("packslip", "", H)]
    DTM_O = ["DTM", "011", check_for_key("curr_date", "", H)]
    N1_SF = [
        "N1",
        "SF",
        check_for_key("manufacturer", "Busse Hospital Disposables", O).upper(),
    ]
    N3_SF = ["N3", check_for_key("manufacturer_addr", "75 Arkay Drive", O).upper()]
    N4_SF = [
        "N4",
        check_for_key("manufacturer_city", "Hauppauge", O).upper(),
        check_for_key("manufacturer_state", "NY", O).upper(),
        check_for_key("manufacturer_zip", "11788", O).upper(),
    ]

    N1_ST = [
        "N1",
        "ST",
        check_for_key("ship_to_name", "", O)
        .upper()
        .replace(r"/", " ")
        .replace("  ", " "),
        "ZZ",
        check_for_key("edi_id", "", O),
    ]
    N3_ST = ["N3", check_for_key("ship_to_addr", "", O).upper()]
    N4_ST = [
        "N4",
        check_for_key("ship_to_city", "", O).upper(),
        check_for_key("ship_to_state", "", O).upper(),
        check_for_key("ship_to_zip", "", O).upper(),
    ]
    HL2 = ["HL", hl_tracker, inner_hl_tracker, "O"]
    PRF = ["PRF", CUST_PO]

    ITEMS = []
    for idx, item in enumerate(I):
        if idx == 0:
            inner_hl_tracker += 1
        hl_tracker += 1
        HL_I = ["HL", hl_tracker, inner_hl_tracker, "I"]

        ITEMS.append(HL_I)

        LOT = check_for_key("lot", "", item)[0]
        LOT_EXP = check_for_key("lot", "", item)[2]

        CUST_ITEM = check_for_key("cust_item", "", item)

        LIN = [
            "LIN",
            check_for_key("line_number", idx + 1, item),
            "VC",
            check_for_key("item", "", item),
            "CB" if CUST_ITEM else "",
            CUST_ITEM if CUST_ITEM else "",
            "ND" if LOT_EXP and LOT else "",
            NATIONAL_DRUG_CODE if LOT_EXP and LOT else "",
            "LT" if LOT_EXP and LOT else "",
            LOT if LOT_EXP and LOT else "",
        ]

        ITEMS.append(LIN)

        SN1 = [
            "SN1",
            check_for_key("line_number", idx + 1, item),
            check_for_key("quantity_ordered_div_by_1000", "", item),
            "CA",
        ]

        ITEMS.append(SN1)

        PID = [
            "PID",
            "F",
            "",
            "",
            "",
            check_for_key("item_description", "", item)
            .upper()
            .replace(r"/", " ")
            .replace("  ", " "),
        ]

        ITEMS.append(PID)

        if LOT and LOT_EXP:
            DTM = ["DTM", "036", check_for_key("lot", "", item)[2].replace("-", "")]
            ITEMS.append(DTM)

    CTT = ["CTT", len(I)]

    ISA = [
        "ISA",
        "00",
        "          ",
        "00",
        "          ",
        "01",
        "002418234T     ",
        "01",
        "012430880      ",
        f"{date[2:]}",
        f"{time[:4]}",
        "U",
        "00400",
        isa_id,
        "0",
        "P",
        ">",
    ]

    GS = [
        "GS",
        "SH",
        "002418234T",
        "012430880",
        date,
        time[:4],
        gs_id,
        "X",
        "004010",
    ]

    GE = ["GE", "1", gs_id]

    IEA = ["IEA", "1", isa_id]

    output = [
        ISA,
        GS,
        ST,
        BSN,
        HL1,
        TD1,
        TD5,
        REF,
        REF_2,
        DTM_O,
        N1_SF,
        N3_SF,
        N4_SF,
        N1_ST,
        N3_ST,
        N4_ST,
        HL2,
        PRF,
        *ITEMS,
        CTT,
    ]

    SE = ["SE", len(output) + 1 - 2, st_id]

    output.append(SE)
    output.append(GE)
    output.append(IEA)

    output_str = "\n".join(["*".join([str(x) for x in y]) for y in output])

    FILENAME = check_for_key("cust_po", "", H)
    # write to file
    with open(rf"c:\\temp\\{FILENAME}.edi", "w", encoding="utf-8") as f:
        f.write(output_str)

    return output_str


if __name__ == "__main__":
    import asyncio
    import sys

    loop = asyncio.get_event_loop()

    if len(sys.argv) > 1:
        if sys.argv[1] == "test_get_lot_by_lot":
            result = loop.run_until_complete(get_lot_by_lot("2230789"))
            print(result)
        elif sys.argv[1] == "test_parse_856_input":
            print(HS_ASN[-1])
            result = loop.run_until_complete(parse_856_input(HS_ASN[-1]))
            print(result)
        elif sys.argv[1] == "nanoid":
            print(generate("1234567890", 9))
        elif sys.argv[1] == "gen_from_output":
            for order in HS_ASN:
                print(order)
                output = loop.run_until_complete(parse_856_input(order))
                print(generate_856_from_output(output))
    else:
        print("Done.")
