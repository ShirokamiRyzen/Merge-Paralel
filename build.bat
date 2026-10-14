@echo off
setlocal
cd /d "%~dp0"

echo ============================================================
echo   BUILD  MergeSortGUI.exe  (memakai PyInstaller)
echo ============================================================
echo.

REM ---- 1) Cari Python ----
set "PY=python"
python --version >nul 2>nul
if errorlevel 1 set "PY=py"

%PY% --version >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python tidak ditemukan di PATH.
    echo         Install Python 3.8+ lalu centang "Add Python to PATH".
    echo.
    pause
    exit /b 1
)
echo [1/4] Python terdeteksi:
%PY% --version

REM ---- 2) Pastikan PyInstaller ----
echo.
echo [2/4] Memeriksa PyInstaller...
%PY% -m PyInstaller --version >nul 2>nul
if errorlevel 1 (
    echo       PyInstaller belum terpasang, memasang...
    %PY% -m pip install --upgrade pyinstaller
)

REM ---- 3) Dependensi opsional (numpy, zstandard) ----
echo.
echo [3/4] Memasang dependensi dari requirements.txt ...
if exist requirements.txt (
    %PY% -m pip install -r requirements.txt
) else (
    echo       requirements.txt tidak ditemukan, dilewati.
)

REM ---- 4) Build .exe ----
echo.
echo [4/4] Membangun .exe (onefile, tanpa jendela konsol)...
%PY% -m PyInstaller --noconfirm --clean --onefile --windowed --name MergeSortGUI gui_app.py
if errorlevel 1 (
    echo.
    echo [ERROR] Build gagal. Periksa pesan di atas.
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo   SELESAI!
echo   Hasil: dist\MergeSortGUI.exe
echo ============================================================
echo.
pause
endlocal
