RE_NUMBER ok
belegnummer ok
rechnungsdatum	ok
quelle ok
mandant ok
invoice_owner ok
sollkonto ok
habenkonto ok
betrag ok
text ok
export_datum ok
pos_label ok
pos_number ok					

/* Ein Schlüssel könnte aus belegnummer, positionsnummer und rechnungsdatum bestehen. */
/* Das IMPORT Programm müsste eigenständig anhand einer Schlüsseldefinition feststellen, ob die betreffenden Datensätze bereits in der Datenbank existieren.*/

/* In der Konfigurationsdatei des IMPORT Programms müsste zusätzliche Einstellung möglich sein. 
 * In dieser Einstellung muss ein Art Check Unique Key before IMPORT Regel definiert
 * Diese Regel muss von einer weiteren Regel umschlossen sein. Die den heißen könnte IMPORT ONLY ROWS WITH NON EXIST KEY
 * werden können. Mein Vorschlag ist hier den Primärschlüssel aus Belegnummer, positionsnr und rechnungsdatum zu bilden.
 * Diese Regel müsste in der XML Konfigurationsdatei optional anzugeben sein. Dementsprechend müsste die XML Schema Definition Datei angepasst werden.
 */

/* 
 * In der Konfigurationsdatei der Anwendung dbtocsv, sollte explizit angegeben werden können, ob eine Datei überschrieben werden soll.
 */
CREATE TABLE rechnung (
  id_rechnung_nr integer not null auto_increment,
  id_position_nr integer,
  belegnummer varchar(200),
  position varchar(1000),
  rechnungs_datum date ,
  quelle varchar(100),
  mandant varchar(100),
  rechnung_inhaber varchar(120),
  konto_soll varchar(45) ,
  konto_haben varchar(45) ,
  betrag decimal(10,2) ,
  beleg_text varchar(1000),
  export_datum date,
  imported boolean default 0,
  PRIMARY KEY (id_rechnung_nr)
) ;


