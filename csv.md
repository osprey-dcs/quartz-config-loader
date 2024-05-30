Each row represents a channel and each non-identifying column represents a domain. Domain column headers map to \<DOMAIN\> in the signal name pattern (directly and indirectly) and are case sensitive.

CSV Table Headers
| Column Header     | Description                       | Data Type |
| -------           | -------                           | -----     |
| CHASSIS           | Chassis/ Node number              | int       |
| CONNECTOR         | Chassis's Connector ID            | int       |
| CHANNEL           | Chassis's Channel number          | int [1-32]|
| SIGNAL            | Signal number                     | int       |
| USE               | Is this channel enabled?          | str [yes, no]|
| CUSTNAM           | Full channel name with Customer-requested designator| str                                                     |
| DESC              | UFF58 requirement                 | str       |
| IDLINE5           | UFF58 requirement                 | str       |
| RESPNODE          | UFF58 requirement                 | str       |
| EGU               | Engineering Units                 | str       |
| ESLO              | Slope in engineering units (EGU/V)| float [0.0]|
| EOFF              | Offset in engineering units (EGU) | float [0.0]|
| LOLOlim           | Low alarm                         | float [0.0]|
| LOlim             | Low warning                       | float [0.0]|
| HIlim             | High warning                      | float [0.0]|
| HIHIlim           | High alarm                        | float [0.0]|