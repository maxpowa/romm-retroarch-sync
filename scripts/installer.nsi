; NSIS Installer Script for RomM-RetroArch Sync
; This script creates an MSI-like installer for the Windows .exe

!include "MUI2.nsh"

; Basic settings
Name "RomM-RetroArch Sync"
OutFile "dist\RomM-RetroArch-Sync-v1.5.0-installer.exe"
InstallDir "$PROGRAMFILES\RomM-RetroArch-Sync"
InstallDirRegKey HKLM "Software\RomM-RetroArch-Sync" "Install_Dir"

; Request admin privileges
RequestExecutionLevel admin

; Modern UI settings
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_LANGUAGE "English"

; Installer sections
Section "Install"
  SetOutPath "$INSTDIR"
  
  ; Copy executable
  File "dist\RomM-RetroArch-Sync.exe"
  
  ; Copy assets
  SetOutPath "$INSTDIR\assets\icons"
  File /r "assets\icons\*.*"
  
  ; Copy JSON data
  SetOutPath "$INSTDIR"
  File "romm_platform_slugs.json"
  
  ; Create Start Menu shortcuts
  SetShellVarContext all
  SetOutPath "$SMPROGRAMS\RomM-RetroArch Sync"
  CreateDirectory "$SMPROGRAMS\RomM-RetroArch Sync"
  CreateShortCut "$SMPROGRAMS\RomM-RetroArch Sync\RomM-RetroArch Sync.lnk" "$INSTDIR\RomM-RetroArch-Sync.exe" "" "$INSTDIR\assets\icons\romm_icon.ico"
  CreateShortCut "$SMPROGRAMS\RomM-RetroArch Sync\Uninstall.lnk" "$INSTDIR\uninstall.exe"
  
  ; Create Desktop shortcut (optional)
  CreateShortCut "$DESKTOP\RomM-RetroArch Sync.lnk" "$INSTDIR\RomM-RetroArch-Sync.exe" "" "$INSTDIR\assets\icons\romm_icon.ico"
  
  ; Register uninstaller
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\RomM-RetroArch-Sync" "DisplayName" "RomM-RetroArch Sync"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\RomM-RetroArch-Sync" "UninstallString" "$INSTDIR\uninstall.exe"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\RomM-RetroArch-Sync" "DisplayIcon" "$INSTDIR\assets\icons\romm_icon.ico"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\RomM-RetroArch-Sync" "Publisher" "RomM-RetroArch Sync"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\RomM-RetroArch-Sync" "DisplayVersion" "1.5.0"
  
  ; Write installation directory to registry
  WriteRegStr HKLM "Software\RomM-RetroArch-Sync" "Install_Dir" "$INSTDIR"
  
  ; Create uninstaller
  WriteUninstaller "$INSTDIR\uninstall.exe"
SectionEnd

; Uninstaller section
Section "Uninstall"
  ; Delete installed files
  Delete "$INSTDIR\RomM-RetroArch-Sync.exe"
  Delete "$INSTDIR\romm_platform_slugs.json"
  Delete "$INSTDIR\uninstall.exe"
  
  ; Delete directories
  RMDir /r "$INSTDIR\assets"
  RMDir /r "$INSTDIR"
  
  ; Delete Start Menu shortcuts
  SetShellVarContext all
  RMDir /r "$SMPROGRAMS\RomM-RetroArch Sync"
  
  ; Delete Desktop shortcut
  Delete "$DESKTOP\RomM-RetroArch Sync.lnk"
  
  ; Delete registry keys
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\RomM-RetroArch-Sync"
  DeleteRegKey HKLM "Software\RomM-RetroArch-Sync"
SectionEnd
