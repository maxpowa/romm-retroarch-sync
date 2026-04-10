# Quick Windows Build Script
# 
# Usage:
#   - Using Anaconda (Recommended):
#     ./scripts/build.ps1
#   
#   - Using MSYS2:
#     ./scripts/build.ps1 -UseMSYS2
#
# Prerequisites: 
#   - Anaconda/Miniconda OR MSYS2 installed
#   - PyInstaller and dependencies

param(
    [Switch]$UseMSYS2,
    [Switch]$BuildInstaller,
    [Switch]$Help
)

if ($Help) {
    Write-Host @"
RomM-RetroArch Sync Windows Build Script
========================================

Usage: .\scripts\build.ps1 [options]

Options:
  -UseMSYS2           Use MSYS2 instead of Anaconda
  -BuildInstaller     Also create NSIS installer after building exe
  -Help               Show this help message

Examples:
  # Build with Anaconda (default)
  .\scripts\build.ps1

  # Build with MSYS2 and create installer
  .\scripts\build.ps1 -UseMSYS2 -BuildInstaller

  # Build in MSYS2 only
  .\scripts\build.ps1 -UseMSYS2
"@
    exit 0
}

Write-Host "🚀 RomM-RetroArch Sync Windows Build" -ForegroundColor Cyan
Write-Host "=====================================`n"

if ($UseMSYS2) {
    Write-Host "📦 Using MSYS2 environment" -ForegroundColor Green
    Write-Host ""
    Write-Host "⚠️  Note: MSYS2 MinGW 64-bit terminal is required!"
    Write-Host "         Close this window and run from MSYS2 MinGW 64-bit terminal:`n"
    Write-Host "         python scripts/build_windows.py`n"
    exit 1
} else {
    Write-Host "📦 Using Anaconda environment" -ForegroundColor Green
    
    # Check if conda is available
    $condaCmd = Get-Command conda -ErrorAction SilentlyContinue
    if (-not $condaCmd) {
        Write-Host "❌ conda not found. Please install Miniconda or Anaconda first." -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✅ Conda found`n"
    
    # Create/activate environment
    Write-Host "📋 Checking build environment..." -ForegroundColor Cyan
    
    $envExists = & conda env list | Select-String "romm-build"
    
    if (-not $envExists) {
        Write-Host "   Creating 'romm-build' environment..."
        & conda create -n romm-build python=3.10 -y
        
        if (-not $?) {
            Write-Host "❌ Failed to create environment" -ForegroundColor Red
            exit 1
        }
    }
    
    Write-Host "   Activating environment..."
    & conda activate romm-build
    
    # Install dependencies
    Write-Host "`n📦 Installing GTK4 and dependencies..." -ForegroundColor Cyan
    & conda install -c conda-forge gtk=4 libadwaita pygobject -y
    
    if (-not $?) {
        Write-Host "❌ Failed to install GTK4" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "`n📦 Installing Python build tools..." -ForegroundColor Cyan
    & pip install -r requirements-build.txt
    
    if (-not $?) {
        Write-Host "❌ Failed to install requirements" -ForegroundColor Red
        exit 1
    }
    
    # Build executable
    Write-Host "`n🔨 Building executable..." -ForegroundColor Cyan
    & python scripts/build_windows.py
    
    if (-not $?) {
        Write-Host "❌ Build failed" -ForegroundColor Red
        exit 1
    }
    
    # Optional: Build installer
    if ($BuildInstaller) {
        Write-Host "`n📦 Building installer..." -ForegroundColor Cyan
        
        $nsis = Get-Command makensis -ErrorAction SilentlyContinue
        if (-not $nsis) {
            Write-Host "⚠️  NSIS not found. Install from: https://nsis.sourceforge.io/" -ForegroundColor Yellow
            Write-Host "   To build installer later, run: makensis scripts/installer.nsi`n" -ForegroundColor Yellow
        } else {
            & makensis scripts/installer.nsi
            
            if ($?) {
                Write-Host "✅ Installer created: dist\RomM-RetroArch-Sync-v1.5.0-installer.exe" -ForegroundColor Green
            } else {
                Write-Host "⚠️  Installer build had issues" -ForegroundColor Yellow
            }
        }
    }
    
    Write-Host "`n✅ Build complete!" -ForegroundColor Green
    Write-Host "   Executable: dist\RomM-RetroArch-Sync.exe`n" -ForegroundColor Green
}
