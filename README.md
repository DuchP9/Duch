# Duch

Test Sente

Z powodu u¿ywania na codzieñ bazy Firebird 2.5 (x32) mia³em problem z konfiguracj¹ wersji Firebird 5.0.
Aplikacja jest budowana do wersji x32 bit (ograniczenie narzucone przez IBExpert)

Uda³o siê wykonaæ zadanie na wersji Firebird 5.0 x32, ale do bazy ³¹czê siê lokalnie (bez uzycia serwera "remote" i localhosta)

komendy których u¿ywa³em do wywo³ania programu (nie zmieniam œcie¿ek):

1. eksport skryptów z bazy:
   dotnet run -- export-scripts --connection-string "Database=C:\_BAZY\DUCH.FDB;User=SYSDBA;Password=masterkey;Dialect=3;Charset=UTF8;ServerType=1;ClientLibrary=C:\Program Files (x86)\Firebird\Firebird_5_0_32\fbclient.dll;" --output-dir "C:\_meta\scripts"

2. za³o¿enie bazy ze skryptów:
   dotnet run -- build-db --db-dir "C:\_BAZY\DUCH_kopia.fdb" --scripts-dir "C:\_meta\scripts"

3. aktualizacja bazy ze skryptów:
   dotnet run -- update-db --connection-string "Database=C:\_BAZY\DUCH.FDB;User=SYSDBA;Password=masterkey;Dialect=3;Charset=UTF8;ServerType=1;ClientLibrary=C:\Program Files (x86)\Firebird\Firebird_5_0_32\fbclient.dll;" --scripts-dir "C:\_meta\scripts"

Punkt 3. (update) mo¿e byæ realizowany z u¿yciem skryptów wygenerowanych w punkcie 1. tylko w przypadku procedur ze wzglêdu na sk³adniê (CREATE OR ALTER PROCEDURE.......), natomiast skrypty do aktualizacji tabel i domen trzeba dostosowaæ (sk³adnia skryptów CREATE TABLE....... i CREATE DOMAIN...........).
Nie do koñca zrozumia³em czy aktualizacja musi odbywaæ siê równie¿ z u¿yciem tych skryptów które wygenerowa³em, wiêc zostawiam to w tej formie. Jeœli zamienimy rêcznie skrypt na "ALTER TABLE" lub "ALTER DOMAIN" to powinno zadzia³aæ.

W folderze \_BAZY jest baza oryginalna oraz skopiowana.
W folderze scripts s¹ skrypty wygenerowane z mojej bazy.
