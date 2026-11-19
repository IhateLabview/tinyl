@echo off
rem Startet das PC-Werkzeug immer mit Python 3.11 (dort sind die Pakete installiert).
rem Beispiel:  robustear live --channel L --gain 30
py -3.11 "%~dp0robustear_pc.py" %*
