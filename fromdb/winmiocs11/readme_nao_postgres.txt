------------------------------------------------------------------------------------------------------
                                    ZUGANGSDATEN POSTGRES-SQL
------------------------------------------------------------------------------------------------------


Die Postgres-SQL-Datenbank verwaltet Stammdaten und Softwarekonfigurationen für die Zentrale Schneid
Software. Die Datenbank ist keine öffentliche Datenbank, und Benutzername und Passwort sind bei Schneid-
Systemen einfache Default-Werte. Für diese Datenbank werden folgende Default-Werte verwendet:
-   username="postgres"
-   password="manager"
-   host="localhost"


------------------------------------------------------------------------------------------------------
                            RELEVANTE METADATEN – DATENBANKINHALTE POSTGRES-SQL
------------------------------------------------------------------------------------------------------


Datenbank – Schema: "siocs", Tabelle: "lognote"
1.  Spalte: "text" (String):
    a.  Aufbau Datenbankeintrag für alle Notizen, die mit "WinMiocs Notiz: <Regler-ID>" beginnen:
        Alle Einträge, die mit "WinMiocs Notiz:" starten, sind im Text direkt einem Regler zugeordnet –
        durch die folgende Nummer, z.B. "WinMiocs Notiz: 17"; hier wäre die Regler-ID "17".
        Wichtig: Für die Gerätezuweisung soll diese Regler-ID aus dem Text jedoch nicht verwendet werden;
        die maßgebliche Zuweisung erfolgt über die Verknüpfung lognote.log_id → logbook.id und dort
        über das Feld param.device.

        Danach kommt der Dateiname der Notiz, z.B. "WinMiocs Notiz: 17\\Parameter Änderungen.txt"; hier
        wäre der Dateiname: "Parameter Änderungen.txt". Hinter diesem Dateinamen folgen die Inhalte der
        Textdatei nach dem ersten Zeilenumbruch "\n".

        Standardannahme für die Verarbeitung: "\n" wird wie bei 1.a/1.b als Zeilenumbruch der Notiz
        behandelt. Falls jedoch nach einem "\n" kein Datum am Anfang der folgenden Zeile steht, wird
        dieser Umbruch inkl. Text als Fortsetzung des vorherigen Eintrags mitgenommen (weil es sonst ein
        unvollständiger Eintrag sein könnte). In diesem Fall kann der Zeitpunkt aus "tst" als Datum/Zeit
        für diesen (fortgesetzten) Eintrag herangezogen werden.

        Die Datei hat keinen festen Aufbau. Die häufigste Form ist, dass in jeder neuen Notiz ein Datum
        am Anfang steht (hin und wieder auch dazwischen).
    b.  Aufbau Datenbankeintrag für alle Notizen, die mit "WinMiocs Notiz: Notiz" beginnen:
        Diese Einträge sind – bis auf die Zuweisung zu einem Regler – identisch zu Punkt 1. Zu
        welchem Bereich diese Notiz gehört, kann ggf. dem Dateinamen entnommen werden. Diese
        Notizen sind meist keinem einzelnen Gerät zuzuordnen.
    c.  Alle Datenbankeinträge, die nicht mit 1 oder 2 beginnen, werden ebenfalls wie 1.a/1.b behandelt
        (Aufteilung nach "\n" als Zeilenumbrüche). Falls nach einem "\n" kein Datum am Anfang der
        folgenden Zeile steht, wird dieser Umbruch inkl. Text als Fortsetzung des vorherigen Eintrags
        mitgenommen (siehe 1.a). Falls auch danach kein Datum vorkommt, kann "tst" als Zeitpunkt
        verwendet werden.
2.  Spalte: "log_id" (Integer):
    a.  Die log_id ist die Verknüpfung zur Tabelle "logbook", in welcher die Regler-ID ("device") zu
        finden ist. Daher ist dieses Feld notwendig, um die Gerätezuweisung zu machen.
3.  Spalte: "loginname" (String):
    a.  Zusatzinformation, die ggf. als Info verwendet werden kann.
4.  Spalte: "tst" (String):
    a.  Enthält den Zeitpunkt der letzten Änderung in der Formatierung "2026-01-10 06:40:01.335665".
    b.  Dieser Zeitpunkt kann als Fallback für den "Zeitpunkt" eines Texteintrages (Spalte: "text")
        verwendet werden, falls am Anfang eines (Teil-)Eintrags kein Datum steht. Das betrifft insbesondere
        Zeilen, die nach "\n" keinen Datumsbeginn haben und daher als Fortsetzung des vorherigen Eintrags
        mitgeführt werden (siehe 1.a/1.c).
Notiz: Anhand der Spalte "tst" kann geprüft werden, ob sich überhaupt etwas geändert hat.
        Wenn "tst" neuer ist als die letzte Prüfung, sollte der komplette Text (Spalte "text") erneut
        ausgewertet und die lokale Merkhilfe (z.B. bereits übertragene/zusammengefasste Einträge) angepasst
        werden.

        Für 1.a/1.b/1.c gilt: Es muss lokal festgehalten werden, welche Einträge bereits synchronisiert
        wurden, damit nicht jedes Mal alles erneut übertragen wird. Wenn sich eine bereits bekannte Notiz
        nachträglich ändert (auch in der Mitte), wird diese Änderung im Zielsystem ggf. nicht als Update,
        sondern als neuer Eintrag behandelt (weil die alte Version bereits übertragen wurde). "tst" dient
        dabei als Signal, dass eine Neubewertung notwendig ist.

        Wenn nach einer bereits vollständig übertragenen Notiz später am Ende ein neuer Zeilenumbruch ohne
        Datum hinzukommt, wird dieser als neuer (zusätzlicher) Eintrag übernommen und mit dem Zeitpunkt aus
        "tst" versehen – ohne die bereits übertragenen Inhalte erneut zu verwenden.


Datenbank – Schema: "siocs", Tabelle: "logbook"
1.  Spalte: "id" (Integer):
    Diese Spalte verknüpft die Tabelle "lognote" mit dieser Tabelle: Spalte "id" in dieser Tabelle ist
    die Spalte "log_id" in der Tabelle "lognote". Diese Verknüpfung ist notwendig, um für die Tabelle
    "lognote" die Regler-ID je Eintrag zu ermitteln. Es kann vorkommen, dass eine "log_id" aus der Tabelle
    "lognote" nicht in der Spalte "id" existiert; das bedeutet, die Notiz ist keinem Gerät zugewiesen.
2.  Spalte: "param" (Objekt), Feld: "device" (String):
    Hinweis: Die Regler-ID liegt in manchen Tabellen als String und in anderen als Integer vor (siehe z.B. "node").
    Diese Spalte enthält ein Objekt mit mehreren Einträgen. In diesem Objekt gibt es das Feld "device"
    (param.device, bzw. {"param": {"device": <Regler-ID>}}). Der Wert in "device" ist die Regler-ID,
    welche für die Gerätezuweisung benötigt wird. Falls der Eintrag (das Feld) "device" nicht vorhanden ist,
    ist die Notiz keinem Gerät zugewiesen (Feld nicht immer vorhanden).
Notiz: Diese Tabelle ist nur relevant, damit die Gerätezuweisung der Notizen aus der Tabelle "lognote"
       erfolgen kann.


Datenbank – Schema: "siocs", Tabelle: "partner"
1.  Spalte: "id" (String):
    Diese Spalte enthält die Regler-ID; anhand dieser Spalte kann das Gerät zugewiesen werden.
2.  Spalte: "name" (String):
    Diese Spalte enthält den Namen der verantwortlichen Person für das Gerät, falls vorhanden.
3.  Spalte: "address" (String):
    Diese Spalte enthält die Adresse des Geräts, falls vorhanden.
4.  Spalte: "attr" (Objekt), Feld: "LAT" (String):
    Geoinformation Latitude (Feld nicht immer vorhanden).
5.  Spalte: "attr" (Objekt), Feld: "LON" (String):
    Geoinformation Longitude (Feld nicht immer vorhanden).
6.  Spalte: "attr" (Objekt), Feld: "UNION" (String):
    Beinhaltet die Information, zu welcher Gruppe das Gerät gehört (Feld nicht immer vorhanden).
Notiz: Bei einer Synchronisierung müssten die bereits übertragenen Informationen irgendwo festgehalten 
       oder über eine API von NAO abgerufen werden, damit auf keinen Fall bestehende Informationen im 
       Zielsystem immer wieder überschrieben werden. Dabei muss immer erst überprüft werden, ob sich 
        irgendeiner der Werte geändert hat, und erst dann wird synchronisiert.


Datenbank – Schema: "winmiocs", Tabelle: "cntswap"
1.  Spalte: "time" (String):
    Spalte mit einem Zeitpunkt in der Formatierung "2026-02-10 00:00:02". Diese Spalte gibt den Zeitpunkt
    eines Zählerwechsels an.
2.  Spalte: "node" (Integer):
    Diese beinhaltet die Regler-ID (für die Gerätezuweisung, aber diesmal als INT) und wird benötigt,
    um den Zählerwechsel einem Gerät zuzuweisen.
3.  Spalte: "ser" (String):
    Diese Spalte enthält die Zählernummer vom Zähler vor dem Zählerwechsel. Dieser Wert MUSS in Integer
    umgewandelt werden, bevor er weiterverwendet wird.
4.  Spalte: "ser_new" (String):
    Diese Spalte enthält die Zählernummer vom Zähler nach dem Zählerwechsel (Zählernummer des neuen Zählers).
    Dieser Wert MUSS in Integer umgewandelt werden, bevor er weiterverwendet wird.
Notiz: Der Zeitpunkt in Kombination mit der Regler-ID muss irgendwo festgehalten werden, damit überprüft
       werden kann, ob etwas Neues synchronisiert werden soll. Der Zeitpunkt allein kann NICHT für eine
       Vorab-Überprüfung verwendet werden, da die Uhrzeit in der Praxis nicht eindeutig ist (sie kann z.B.
       immer als "00:00:02" abgelegt sein). Dadurch kann es passieren, dass am gleichen Tag mehrere
       Zählerwechsel stattfinden, aber beim Trigger-Vergleich ("bis zu diesem Zeitpunkt bereits synchron")
       ein späterer Wechsel nicht mehr erkannt wird. Daher immer die Paarung Zeitpunkt + Regler-ID für die
       Überprüfung verwenden.

       Zusätzlich: Beim Weiterverarbeiten darf die Zählernummer nicht in eine Darstellung geraten, die zu
       Rundungen führt (z.B. wissenschaftliche Notation wie "1e+9"). Falls ein String-Format benötigt wird
       (z.B. für Telegraf/Line-Protocol), muss die Zahl als vollständiger Integer-Wert übertragen werden.


------------------------------------------------------------------------------------------------------
                        RELEVANTE ZEITREIHENDATEN – DATENBANKINHALTE POSTGRES-SQL
------------------------------------------------------------------------------------------------------


Datenbank – Schema: "cnt"
1.  Spalte: "time" (String):
    Spalte mit einem Zeitpunkt in der Formatierung "2026-02-10 00:00:02". Diese Spalte gibt den Zeitpunkt
    der Aufzeichnung (bzw. Messzeitpunkt) an.
2.  Spalte: "node" (Integer):
    Diese beinhaltet die Regler-ID (für die Gerätezuweisung, aber diesmal als INT) und wird benötigt,
    um den Messwert einem Gerät zuzuweisen.
3.  Spalte: "ser" (String):
    Diese Spalte enthält den Wert, der aufgezeichnet werden soll. Es handelt sich um eine fortlaufende
    Erfassung der Zählernummer. Dieser Wert MUSS in Integer umgewandelt werden, bevor er weiterverwendet
    wird.
Notiz: Die Erfassung der Zeitreihe für die Zählernummer erfolgt nur deshalb über die Postgres-Datenbank
       statt über die CSV-Dateien (wo die anderen Messdaten liegen), weil Schneid einen Bug hat, bei dem
       die Nachkommastellen in der CSV-Datei gerundet werden. Die Synchronisierung dieser Datenbanktabelle
       erfolgt – wie die Metadaten (Stammdaten) der Postgres-SQL-Datenbank – in der Regel nur einmal am Tag.

       Zusätzlich gilt: Beim Weiterverarbeiten darf die Zählernummer nicht in eine Darstellung geraten, die
       zu Rundungen führt (z.B. wissenschaftliche Notation wie "1e+9"). Falls ein String-Format benötigt
       wird (z.B. für Telegraf/Line-Protocol), muss die Zahl als vollständiger Integer-Wert übertragen werden.



