import csv
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET

from p4p.client.thread import Context

"""Reads the CCCR Configuration csv file.

    Parameters:
    input_filepath (str): The file location of the CCCR csv
    output_filepath (str): The file location to write output CCCR csv

    Returns:
    output_file (.csv): A csv of the values that were used to _put_ into the EPICS DB
"""

# Initialize logger
_log = logging.getLogger(__name__)

# TODO: check for CUSTNAME duplicates
# TODO: get values -> output table
# TODO: ignore cells with NONE or blanks
# TODO: validate input against domain type and raise exceptions
# TODO: differentiate between debugging and info to print
# CONSIDER: update CSV (IDLINE5 -> DESC5, SPECDATATYP -> SDTYP)

class PatternSettings:
    # The default valid_input must be the first element of the list
    """NAME_METADATA and DOMAINS are columns of the CSV input file
    'type' defines how the value shall be converted into a native format in convert_bytype()
    'pattern' defines which pattern shall be used, and how it must be modified to read the csv
    'valid_input' defines which inputs are acceptable, and the default value
    'input_switch' switches the input from the CCCR standard to the EPICS standard
    """

    def __init__(self, prefix:str):
        self.prefix = prefix or ""
        self.signal_pattern = "S<SIGNAL>:CS<CHASSIS>:CH<CHANNEL>:CN<CONNECTOR>" # Input signal pattern
        self.record_pattern = f"{self.prefix}<CHASSIS>:SA:Ch<CHANNEL>:<DOMAIN>" # EPICS PV pattern
        self.alarm_pattern = f"{self.prefix}<CHASSIS>:ACQ:<DOMAIN>:<CHANNEL>"
        self.filename_record = f"{self.prefix}SA:FILE"
        self.domains = self._build_domains()
        self.domains_list = list(self.domains.keys())
        self.name_metadata = {
            # Overall System CH
            "SIGNAL": {
                "type": "int",
            },
            # Digitizer
            "CHASSIS": {
                "type": "int",
            },
            # Digitizer Channel
            "CHANNEL": {
                "type": "int",
            },
            # Digitizer Connector
            "CONNECTOR": {
                "type": "str",  # [DB1, DB2]
            },
        }

    def _build_domains(self):
        rp = self.record_pattern
        ap = self.alarm_pattern

        return { 
            # Channel Use (Yes/No)
            "USE": {
                "type": "bool",  # EPICS:enum [Yes, No]
                "pattern": rp,
            },
            # Channel User Label
            "CUSTNAM": {
                "type": "str",  # Full Channel Name with Customer-requested designator
                "pattern": rp.replace("<DOMAIN>", "NAME"),
            },
            # Channel Description
            "DESC": {
                "type": "str",  # UFF58 ID Line 2
                "pattern": rp,
            },
            "IDLINE5": {
                "type": "str",  # UFF58 ID Line 5
                "pattern": rp.replace("<DOMAIN>", "DESC5"),
            },
            "RESPNODE": {
                "type": "int",  # UFF58 ID Line 6, field 6
                "pattern": rp,
            },
            # Response Direction
            "RESPDIR": {
                "type": "str",  # UFF58 ID Line 6, field 6
                "pattern": rp,
                "input_switch": {  # with mbbo values
                    "scalar": "Scalar",  # 0, default
                    "default": "Scalar",
                    "X+": "+X Translation",  # 1
                    "Y+": "+Y Translation",  # 2
                    "Z+": "+Z Translation",  # 3
                    "X-": "-X Translation",  # -1
                    "Y-": "-Y Translation",  # -2
                    "Z-": "-Z Translation",  # -3
                    "XR+": "+X Rotation",  # 4
                    "YR+": "+Y Rotation",  # 5
                    "ZR+": "+Z Rotation",  # 6
                    "XR-": "-X Rotation",  # -4
                    "YR-": "-Y Rotation",  # -5
                    "ZR-": "-Z Rotation",  # -6
                },
                # "valid_input": list(DOMAINS["RESPDIR"]["input_switch"].keys())
            },
            # Specific Data Type Field
            "SPECDATATYP": {
                "type": "str",  # UFF58 ID Line 6, field 6
                "pattern": rp.replace("<DOMAIN>", "SDTYP"),
                "valid_input": [
                    "unknown",  # 0, default value
                    "general", "stress", "strain", "temperature", "heat flux",
                    "displacement", "reaction force", "velocity", "acceleration",
                    "excitation force", "pressure", "mass", "time", "frequency", "rpm",
                ],
            },
            # Engineering Unit
            "EGU": {
                "type": "str",
                "pattern": rp,
            },
            # Custom Measurement Location
            # CUSTMEASLOC str
            # Volts to EU slope
            "ESLO": {
                "type": "float",  # EGU/V
                "pattern": rp,
            },
            # Volts to EU offset
            "EOFF": {
                "type": "float",  # EGU
                "pattern": rp,
            },
            # MAXEULVL float
            # SAMPLPERSEC int
            # Low Alarm Limit (in EU)
            "LOLOlim": {
                "type": "float",  # EGU
                "pattern": ap,
            },
            # Low Warning Level (in EU)
            "LOlim": {
                "type": "float",  # EGU
                "pattern": ap,
            },
            # High Warning Level (in EU)
            "HIlim": {
                "type": "float",  # EGU
                "pattern": ap,
            },
            # High Alarm Limit (in EU)
            "HIHIlim": {
                "type": "float",  # EGU
                "pattern": ap,
            },
            # Coupling (AC or DC)
            "COUPLING": {
                "type": "str",  # EPICS:enum [AC, DC]
                "valid_input": [ "AC", "DC", ],
                "pattern": f"{self.prefix}<CHASSIS>:ACQ:coupling:<CHANNEL>",
            },
            # Configuration Timestamp
            # CONFIGTIMEID str
        }

# Signal object representing a row in the CSV
class Signal:
    def __init__(self, config: dict, patterns: PatternSettings):
        self.name = (
            patterns.signal_pattern.replace("<SIGNAL>", config["SIGNAL"])
            .replace("<CHASSIS>", config["CHASSIS"])
            .replace("<CHANNEL>", config["CHANNEL"])
            .replace("<CONNECTOR>", config["CONNECTOR"])
        )
        self.name_dict = {m: config[m] for m in patterns.name_metadata.keys()}
        self.config: dict = config  # csv cfg file row
        self.use: str = config["USE"].lower()
        if self.use == "yes":
            self.records: dict = {
                key: Record(
                    signal=self,
                    signal_cfg=config,
                    domain=key,
                    chassis=config["CHASSIS"],
                    channel=config["CHANNEL"],
                    cfg_value=value,
                    patterns=patterns,
                )
                for key, value in config.items()    # for each column in cfg file
                if key in patterns.domains_list     # if the header appears in the domains list
            }
        else:                                       # if not used, set all to None
            for d in patterns.domains_list:
                config[d] = None

    def signal_todict(self):
        signal_dict = self.name_dict
        records_dict = {}
        if hasattr(self, "records"):
            # records_dict = {k: r.new_value.value for k, r in self.records.items()}
            for k, r in self.records.items():
                # _log.debug(f"{self.name} {k} {r.new_value}")
                records_dict.update({k: r.value})
        else:
            records_dict = self.config
        signal_dict.update(records_dict)
        # _log.debug(f"{signal_dict}")
        return signal_dict


# Record represents one EPICS PV that may be written to
class Record:
    def __init__(
        self,
        signal: Signal,
        signal_cfg: dict,
        domain: str,
        chassis: str,
        channel: str,
        patterns: PatternSettings,
        cfg_value,
    ):
        self.signal: Signal = signal
        self.signal_cfg = signal_cfg
        self.rec_name: str = str(
            patterns.domains[domain]["pattern"]
            .replace("<CHASSIS>", "{:02d}".format(int(chassis)))
            .replace("<CHANNEL>", "{:02d}".format(int(channel)))
        ).replace("<DOMAIN>", domain)
        self.value = self.process_val(
            description=signal_cfg["DESC"] + f":{domain}", 
            value=cfg_value, 
            domain=domain,
            patterns=patterns, 
        )
        self.old_value = None       # GET from EPICS DB
        self.changed: bool = False  # when new_value PUT, compare
        self._new_value = None      # GET after PUT
        self.domain = domain

    @property
    def new_value(self):
        return self._new_value

    @new_value.setter
    def new_value(self, value):
        self._new_value = value
        self.changed = self._new_value != self.old_value

    def __setattr__(self, key, value):
        if key == "new_value":
            self._new_value = value
            self.changed = self._new_value != self.old_value
        else:
            super().__setattr__(key, value)

    def process_val(self, description: str, value, domain: str, patterns: PatternSettings):
        cfg_value = value
        
        try:
            # if the value's domain has a list of valid inputs, verify input
            if "valid_input" in patterns.domains[domain]:
                cfg_value = verify_input(
                    description,
                    cfg_value,
                    patterns.domains[domain]["valid_input"],
                )
            # if the value's domain requires an input switch, apply switch
            if "input_switch" in patterns.domains[domain]:
                cfg_value = apply_input_switch(
                    description,
                    cfg_value,
                    patterns.domains[domain]["input_switch"],
                )

        except ValueError as e:
            if "valid_input" in patterns.domains[domain]:
                valid_inputs = patterns.domains[domain]["valid_input"]
            elif "input_switch" in patterns.domains[domain]:
                valid_inputs = patterns.domains[domain]["input_switch"]
            raise ValueError(f'{e} ({value}) at {self.signal.name} "{description}".\n' + (f'Valid inputs: {valid_inputs}' if valid_inputs else ''))
        
        cfg_value = convert_bytype(
            description=self.signal_cfg["DESC"] + f":{domain}",
            val=cfg_value,
            domain_type=patterns.domains[domain]["type"],
        )

        # _log.debug(f"new value: {cfg_value}")
        return cfg_value


def convert_bytype(description: str, val, domain_type: str):
    """Converts the data type of the given value based on the record's domain's type.
    Specifically designed.

    Parameters
    description (str): User provided name to alert of value mismatch
    val (any): Value from the CCCR to convert
    domain_type (str): The record's domain's data type to convert to

    Returns
    val ([integer, string, float, boolean]): Returned value of new data type according
    to the list available
    """
    _log.debug(f"Converting {description}: {val} to {domain_type}")
    if domain_type == "int":
        # _log.debug("val is int")
        return 0 if val.upper() == "NONE" else int(val)
    elif domain_type == "str":
        # _log.debug("val is str")
        return str(val).strip()
    elif domain_type == "float":
        # _log.debug("val is float")
        return 0 if val.upper() == "NONE" else float(val)
    elif domain_type == "bool":
        # _log.debug("val is bool")
        if val.lower() == "yes":
            return "Yes"
        elif val.lower() == "no":
            return "No"
    raise ValueError(f"Could not convert {domain_type!r} {description!r}:{val!r}")


def verify_input(description: str, val: str, valid_inputs: list) -> str:
    """Verifies the given input data is within the valid input list. Otherwise if it is 
    blank, it returns 'default', else "INVALID_INPUT".

    Parameters
    description (str): User provided name to alert of value mismatch
    val (str): String value from the CCCR to check
    valid_inputs (list): List of valid inputs

    Returns
    valid_value[0] (str): First matched valid input, or default valid input
    """
    _log.debug(f"Verifying input for {description}: {val}")
    valid_val = [option for option in valid_inputs if option.lower() == val.lower()]
    if not val or val.strip() == "":
        return_val = valid_inputs[0]
    elif valid_val:
        return_val = valid_val[0]
    else:
        raise ValueError("Invalid input")
    return return_val


def apply_input_switch(description: str, val: str, input_switch: dict) -> str:
    """Switches CSV input to EPICS DB input, if a switch exists for that domain.

    Parameters
    description (str): User provided name to alert of value mismatch
    val (str): String value from the CCCR to check
    input_switch (dict): Input switch

    Returns
    switched_val (str): Value switched from CSV standard to EPICS DB standard
    """
    _log.debug(f"Applying input switch for {description}: {val}")
    if not val or val.strip() == "":
        switched_val = input_switch["default"]
    elif val in list(input_switch.keys()):
        switched_val = input_switch[val]
    else:
        raise ValueError("Invalid input")
    return switched_val

def getargs():
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Process csv configuration file")
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        dest="input_filepath",
        required=False,
        help="Input file's filepath",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        dest="output_path",
        required=False,
        help="Path to the output folder",
    )
    parser.add_argument(
        "-p",
        "--prefix",
        type=str,
        dest="prefix",
        default="FDAS:",
        required=False,
        help="Prefix for records, alarms, and filename (default: 'FDAS:')"
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        required=False,
        help="Run in test mode with dummy values from tests/input/dummy.csv",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="level",
        default=logging.INFO,
        action="store_const",
        const=logging.DEBUG,
        required=False,
        help="Run in verbose mode",
    )
    parser.add_argument(
        "--sim", 
        action="store_true", 
        help="Do not actually change any PVs.",
    )

    return parser


def main():
    configuration = {}
    signals = []

    args = getargs().parse_args()

    logging.basicConfig(
        level=args.level, format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
    )
    _log.setLevel(args.level)

    p4p_logger = logging.getLogger("p4p")
    p4p_logger.setLevel(logging.WARNING)
    p4p_logger.propagate = True

    _log.info(f"Arguments: {args.__dict__}")
    _log.debug(
        f"Input filepath {args.input_filepath}; string: {isinstance(args.input_filepath, str)}"
    )

    if args.test:
        _log.info("Entering test mode")

        tests_path = os.path.dirname(os.path.realpath(__file__)) + "/tests"
        configuration["input_fp"] = f"{tests_path}/input/test.csv"
        configuration["output_fp"] = f"{tests_path}/output/output.csv"

    elif args.input_filepath is None:
        print("No input filepath selected")
        sys.exit(0)

    else:
        if not os.path.exists(args.input_filepath):
            raise FileNotFoundError(f"File {args.input_filepath} not found")
        elif not os.path.isdir(args.output_path):
            raise FileNotFoundError(f"Path {args.output_path} not found")
        else:
            configuration["input_fp"] = args.input_filepath
            configuration["output_fp"] = f"{args.output_path}/output.csv"

    input_fn_with_ext = os.path.basename(configuration["input_fp"])
    inputfn, inputfn_ext = os.path.splitext(input_fn_with_ext)
    configuration["filename"] = inputfn

    patterns = PatternSettings(args.prefix)
    
    _log.info(f"Input file: {configuration['input_fp']}")
    _log.info(f"Output file: {configuration['output_fp']}")
    _log.info(f"Domains to output: {patterns.domains_list} ({len(patterns.domains_list)})")
    _log.debug(f"Record pattern: {patterns.record_pattern}")
    _log.debug(f"Alarm pattern: {patterns.alarm_pattern}")

    """Open the input filepath, read it, convert the values, and append the new row
    to the list of signals."""
    _log.info("Converting values to accepted datatypes and format")
    with open(configuration["input_fp"], newline="") as configuration_csv:
        configuration_table = csv.DictReader(configuration_csv)
        for row in configuration_table:
            s = Signal(dict(row), patterns)
            signals.append(s)
        headers = configuration_table.fieldnames

    """Organize the records by Domain"""
    recs_bydomain = {d: [] for d in patterns.domains_list}
    for dom in patterns.domains_list:
        for s in [s for s in signals if hasattr(s, "records")]:
            for d, r in s.records.items():
                if d == dom:
                    recs_bydomain[dom].append(r)

    # Write records and their values to simulation output csv
    if args.sim:
        _log.warning("Simulation")
        _log.debug(f"Filename {os.path.basename(configuration["filename"])}")
        if args.test:
            with open(f"{tests_path}/output/sim_output.csv", "w", newline="") as sim_output_fp:
                writer = csv.writer(sim_output_fp)
                for d, recs in recs_bydomain.items():
                    for r in recs:
                        writer.writerow([d, r.rec_name, r.value])
        time.sleep(5)  # fake some actual work
        sys.exit(0)

    """By domain, GET old values from EPICS records. PUT values to EPICS records.
    GET new values from EPICS records for output file."""
    ctxt = Context("pva")

    _log.debug(f"Put filename {os.path.basename(configuration["filename"])}")
    ctxt.put(patterns.filename_record, os.path.basename(configuration["filename"]))

    count_changed = 0
    recs_changed = []
    for d, recs in recs_bydomain.items():
        rec_names = [r.rec_name for r in recs]
        rec_vals = [r.value for r in recs]

        _log.info(f"Processing {d}")
        # _log.info(f"Processing {d}: {list(zip(rec_names,rec_vals))}")
        _log.debug("GET old values")
        old_values = ctxt.get(rec_names, throw=False)
        for rec, old_value in zip(recs, old_values):
            if isinstance(old_value , Exception):
                raise RuntimeError(f'Failed to get {rec.rec_name}')
            rec.old_value = old_value

        _log.debug("PUT config values")
        ctxt.put(rec_names, rec_vals)

        # _log.info("GET new values")
        new_values = ctxt.get(rec_names)
        for rec, new_value in zip(recs, new_values):
            rec.new_value = new_value
            if rec.changed:
                count_changed += 1
                recs_changed.append(rec.rec_name)
            # _log.debug(f"{rec.rec_name}, {rec.old_value}, {rec.new_value}, {rec.changed}")

    _log.info(f"Records changed: {count_changed}")
    _log.info("Write output configuration file")
    with open(configuration["output_fp"], "w", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=headers)
        writer.writeheader()
        for s in signals:
            # _log.debug(f"Writing: {s.name}")
            writer.writerow(s.signal_todict())

    # Setup XML Tree with appropriate indentations
    def indent(elem, level=0):
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for elem in elem:
                indent(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i

    pvtable = ET.Element("pvtable", enable_save_restore="true", version="3.0")
    timeout = ET.SubElement(pvtable, "timeout")
    timeout.text = "60.0"
    pvlist = ET.SubElement(pvtable, "pvlist")
    for s in signals:
        if hasattr(s, "records"):
            for k, r in s.records.items():
                # _log.debug(f"add {r.rec_name} to xml")
                pv = ET.SubElement(pvlist, "pv")
                ET.SubElement(pv, "name").text = str(r.rec_name)
                ET.SubElement(pv, "saved_value").text = str(r.value)
    _log.debug("Make tree")
    indent(pvtable)
    tree = ET.ElementTree(pvtable)
    output_xml_fp = os.path.dirname(f"{configuration['output_fp']}/output.xml")
    _log.info("Write xml file")

    with open(output_xml_fp, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)


if __name__ == "__main__":
    main()
