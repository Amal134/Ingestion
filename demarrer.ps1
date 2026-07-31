<#
    Demarrage du pipeline KBO.

    Usage :  .\demarrer.ps1

    Enchaine les trois etapes qui doivent l'etre dans cet ordre : lancer le
    moteur Docker, monter les conteneurs, puis controler que les services
    repondent vraiment. Un port ouvert ne prouve rien : c'est le proxy Docker
    qui ecoute, meme quand le service derriere est mort. D'ou l'appel final a
    verifier_pipeline.py.
#>

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# ----------------------------------------------------- 1. le moteur Docker
Write-Host "`n[1/3] Moteur Docker" -ForegroundColor Cyan

docker ps *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  le demon ne repond pas, lancement de Docker Desktop..."
    $exe = "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
    if (-not (Test-Path $exe)) { throw "Docker Desktop introuvable : $exe" }
    Start-Process $exe

    $pret = $false
    foreach ($i in 1..60) {
        Start-Sleep -Seconds 5
        docker ps *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  demon pret apres $($i * 5)s" -ForegroundColor Green
            $pret = $true
            break
        }
        if ($i % 6 -eq 0) { Write-Host "  ...$($i * 5)s" }
    }
    if (-not $pret) {
        throw "Le demon n'a pas demarre. Si l'erreur est un '500 Internal Server Error', " +
              "verifiez la memoire de la VM : wsl -d docker-desktop -- free -m"
    }
} else {
    Write-Host "  deja demarre" -ForegroundColor Green
}

# ------------------------------------------------------- 2. les conteneurs
Write-Host "`n[2/3] Conteneurs" -ForegroundColor Cyan
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up a echoue" }

# MongoDB expose un healthcheck ; on l'attend avant de sonder quoi que ce soit.
Write-Host "  attente de MongoDB..."
foreach ($i in 1..36) {
    $etat = docker inspect --format "{{.State.Health.Status}}" kbo-mongo 2>$null
    if ($etat -eq "healthy") { Write-Host "  MongoDB pret" -ForegroundColor Green; break }
    Start-Sleep -Seconds 5
}

# --------------------------------------------------------- 3. verification
Write-Host "`n[3/3] Verification des services" -ForegroundColor Cyan
& ".\.venv\Scripts\python.exe" "verifier_pipeline.py"
exit $LASTEXITCODE
