import httpx
from rich import print
import re
from nanoid import generate
from datetime import datetime

from maps.eight_five_five import input_map

import apsw


conn = apsw.Connection("hs_asn.db")
# create table if not exists asn with po as primary key document as text and date sent as text
conn.cursor().execute(
    "CREATE TABLE IF NOT EXISTS asn (po TEXT PRIMARY KEY, document TEXT, date_sent TEXT)"
)


def insert(po: str, document: str, date_sent: str) -> None:
    global conn
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO asn (po, document, date_sent) values (?, ?, ?)",
            (po, document, date_sent),
        )
    except apsw.ConstraintError as e:
        print(e)


async def get_lot_by_lot(lot: str) -> str:
    """Get the lot by lot number and return expiration date"""
    url = f"https://lots.bhd-ny.com/?lot={lot}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        json = {}
        if response.status_code == 200:
            json = response.json()

        return json.get("expiration", None)


def parse_856_raw_export(file: str):
    file_path = rf"{file}"
    with open(file_path, "r") as f:
        input_str = f.read()

    asn = []

    order = ""

    for line in input_str.splitlines():
        if line.startswith('"H"~"HENRYSCHEIN"') and order != "":
            asn.append(order)
            order = ""

        order += line + "\n"

    asn.append(order)
    order = ""

    return asn


def check_for_key(key, value, dict) -> any:
    return dict.get(key, value)


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
    if not re.search(r"(FED|UPS)", check_for_key("scac", "", PS)):
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
        "92",
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
            "",
            "",
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
    ]

    if not re.search(r"(FED|UPS)", check_for_key("scac", "", PS)):
        output.append(REF_2)

    output.extend(
        [
            DTM_O,
            N1_SF,
            N3_SF,
            N4_SF,
            N1_ST,
            N3_ST,
            N4_ST,
            HL2,
            PRF,
        ]
    )

    output.extend(ITEMS)

    output.append(CTT)

    SE = ["SE", len(output) + 1 - 2, st_id]

    output.extend([SE, GE, IEA])

    output_str = "\n".join(["*".join([str(x) for x in y]) for y in output])

    FILENAME = check_for_key("cust_po", "", H)
    # write to file
    # with open(rf"c:\\temp\\{FILENAME}.edi", "w", encoding="utf-8") as f:
    #     f.write(output_str)

    insert(FILENAME, output_str, f"{date} {time}")

    try:
        with open(
            rf"/mnt/evision_out/henry schein/{FILENAME}.edi", "w", encoding="utf-8"
        ) as f:
            f.write(output_str)
    except:
        print("Failed to write to file")

    return output_str


if __name__ == "__main__":
    import asyncio
    import sys
    import os

    loop = asyncio.get_event_loop()

    if len(sys.argv) > 1:
        if sys.argv[1] == "test_get_lot_by_lot":
            result = loop.run_until_complete(get_lot_by_lot("2230789"))
            print(result)

        elif sys.argv[1] == "test_parse_856_input":
            file_path = sys.argv[2]
            assert os.path.exists(file_path), "File does not exist"
            assert os.path.isfile(file_path), "File is not a file"

            asn = parse_856_raw_export(file_path)

            result = loop.run_until_complete(parse_856_input(asn))
            print(result)

        elif sys.argv[1] == "nanoid":
            print(generate("1234567890", 9))

        elif sys.argv[1] == "parse":
            file_path = sys.argv[2]
            assert os.path.exists(file_path), "File does not exist"
            assert os.path.isfile(file_path), "File is not a file"

            asn = parse_856_raw_export(file_path)

            for order in asn:
                output = loop.run_until_complete(parse_856_input(order))
                print(generate_856_from_output(output))

    else:
        print("Done.")
