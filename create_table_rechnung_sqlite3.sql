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

/* Ein Schl�ssel k�nnte aus belegnummer, positionsnummer und rechnungsdatum bestehen. */
/* Das IMPORT Programm m�sste eigenst�ndig anhand einer Schl�sseldefinition feststellen, ob die betreffenden Datens�tze bereits in der Datenbank existieren.*/

/* In der Konfigurationsdatei des IMPORT Programms m�sste zus�tzliche Einstellung m�glich sein. 
 * In dieser Einstellung muss ein Art Check Unique Key before IMPORT Regel definiert
 * Diese Regel muss von einer weiteren Regel umschlossen sein. Die den hei�en k�nnte IMPORT ONLY ROWS WITH NON EXIST KEY
 * werden k�nnen. Mein Vorschlag ist hier den Prim�rschl�ssel aus Belegnummer, positionsnr und rechnungsdatum zu bilden.
 * Diese Regel m�sste in der XML Konfigurationsdatei optional anzugeben sein. Dementsprechend m�sste die XML Schema Definition Datei angepasst werden.
 */

/* 
 * In der Konfigurationsdatei der Anwendung dbtocsv, sollte explizit angegeben werden k�nnen, ob eine Datei �berschrieben werden soll.
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


