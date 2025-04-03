if (-not (Get-Module -ListAvailable -Name PSSQLite)) { 
    Install-Module -Name PSSQLite -Scope CurrentUser
}
Import-Module PSSQLite

$jsonFolderPath = "S:\PBIData\Biztech\Failed_Logins\Interactive_Signins\"
$tableName = "Interactive"
$dbDirectory = [System.Environment]::GetEnvironmentVariable('SQLITEPATH')
if (-not $dbDirectory) {
    $dbDirectory = "C:\Windows\Temp\JSON"
    if (-not (Test-Path $dbDirectory)) {
        New-Item -Path $dbDirectory -ItemType Directory | Out-Null
    }
}
$dbPath = Join-Path -Path $dbDirectory -ChildPath "O365logins.sqlite3"

$logFilePath = Join-Path -Path $dbDirectory -ChildPath "debug_$tableName.log"

# Initialize database
function Initialize-Database {
    param (
        [string]$dbPath
    )

    if (-Not (Test-Path $dbPath)) {
        # Create an empty database file
        $connection = New-Object System.Data.SQLite.SQLiteConnection("Data Source=$dbPath;Version=3;")
        $connection.Open()
        $connection.Close()
    }
}

function Initialize-Table {
    param (
        [string]$dbPath,
        [string]$tableName
    )
    $createTableQuery = @"
    CREATE TABLE IF NOT EXISTS $tableName (
        id TEXT PRIMARY KEY,
        createdDateTime TEXT,
        userDisplayName TEXT,
        userPrincipalName TEXT,
        userId TEXT,
        appId TEXT,
        appDisplayName TEXT,
        ipAddress TEXT,
        clientAppUsed TEXT,
        userAgent TEXT,
        correlationId TEXT,
        conditionalAccessStatus TEXT,
        originalRequestId TEXT,
        isInteractive INTEGER,
        tokenIssuerName TEXT,
        tokenIssuerType TEXT,
        clientCredentialType TEXT,
        processingTimeInMilliseconds INTEGER,
        riskDetail TEXT,
        riskLevelAggregated TEXT,
        riskLevelDuringSignIn TEXT,
        riskState TEXT,
        resourceDisplayName TEXT,
        resourceId TEXT,
        resourceTenantId TEXT,
        homeTenantId TEXT,
        homeTenantName TEXT,
        authenticationRequirement TEXT,
        signInIdentifier TEXT,
        signInIdentifierType TEXT,
        servicePrincipalName TEXT,
        userType TEXT,
        flaggedForReview INTEGER,
        isTenantRestricted INTEGER,
        autonomousSystemNumber INTEGER,
        crossTenantAccessType TEXT,
        uniqueTokenIdentifier TEXT,
        incomingTokenType TEXT,
        authenticationProtocol TEXT,
        signInTokenProtectionStatus TEXT,
        originalTransferMethod TEXT,
        isThroughGlobalSecureAccess INTEGER,
        globalSecureAccessIpAddress TEXT,
        sessionId TEXT,
        appOwnerTenantId TEXT,
        resourceOwnerTenantId TEXT,
        status_errorCode INTEGER,
        status_failureReason TEXT,
        status_additionalDetails TEXT,
        deviceDetail_deviceId TEXT,
        deviceDetail_displayName TEXT,
        deviceDetail_operatingSystem TEXT,
        deviceDetail_browser TEXT,
        deviceDetail_isCompliant INTEGER,
        deviceDetail_isManaged INTEGER,
        deviceDetail_trustType TEXT,
        location_city TEXT,
        location_state TEXT,
        location_countryOrRegion TEXT,
        location_geoCoordinates_latitude REAL,
        location_geoCoordinates_longitude REAL
    );
    CREATE INDEX IF NOT EXISTS idx_id ON $tableName (id);
"@
    try {
        Invoke-SqliteQuery -DataSource $dbPath -Query $createTableQuery
    } catch {
        Write-Host "Failed to create database and table: $_" -ForegroundColor Red
    }
}

function Log-Error {
    param (
        [string]$errorMessage
    )
    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $logMessage = "$timestamp - $errorMessage"
    Add-Content -Path $logFilePath -Value $logMessage
}

Initialize-Database -dbPath $dbPath
Initialize-Table -dbPath $dbPath -tableName $tableName


try {
    # Create and open the connection to the SQLite database
    $connection = New-Object System.Data.SQLite.SQLiteConnection("Data Source=$dbPath;Version=3;")
    $connection.Open()

    # Get all JSON files in the folder
    $jsonFiles = Get-ChildItem -Path $jsonFolderPath -Filter *.json -Recurse

    # Loop through each JSON file
    foreach ($file in $jsonFiles) {
        # Read the JSON file
        $jsonContent = Get-Content -Path $file.FullName -Raw | ConvertFrom-Json

        # Loop through each record in the JSON file
        foreach ($record in $jsonContent) {
            # Escape single quotes in the failureReason field
            $failureReason = $record.status.failureReason -replace "'", "''"

            # Check if the id already exists
            $checkQuery = "SELECT COUNT(*) FROM $tableName WHERE id = '$($record.id)';"
            try {
                $exists = Invoke-SqliteQuery -DataSource $dbPath -Query $checkQuery -As SingleValue
            } catch {
                Write-Host $checkQuery
            }
            if ($exists -eq 0) {
                # Insert the record into the SQLite database
                $query = @"
                INSERT INTO $tableName (
                    id, createdDateTime, userDisplayName, userPrincipalName, userId, appId, appDisplayName, ipAddress, clientAppUsed, userAgent, correlationId, conditionalAccessStatus, originalRequestId, isInteractive, tokenIssuerName, tokenIssuerType, clientCredentialType, processingTimeInMilliseconds, riskDetail, riskLevelAggregated, riskLevelDuringSignIn, riskState, resourceDisplayName, resourceId, resourceTenantId, homeTenantId, homeTenantName, authenticationRequirement, signInIdentifier, signInIdentifierType, servicePrincipalName, userType, flaggedForReview, isTenantRestricted, autonomousSystemNumber, crossTenantAccessType, uniqueTokenIdentifier, incomingTokenType, authenticationProtocol, signInTokenProtectionStatus, originalTransferMethod, isThroughGlobalSecureAccess, globalSecureAccessIpAddress, sessionId, appOwnerTenantId, resourceOwnerTenantId, status_errorCode, status_failureReason, status_additionalDetails, deviceDetail_deviceId, deviceDetail_displayName, deviceDetail_operatingSystem, deviceDetail_browser, deviceDetail_isCompliant, deviceDetail_isManaged, deviceDetail_trustType, location_city, location_state, location_countryOrRegion, location_geoCoordinates_latitude, location_geoCoordinates_longitude
                ) VALUES (
                    '$($record.id)', '$($record.createdDateTime)', '$($record.userDisplayName)', '$($record.userPrincipalName)', '$($record.userId)', '$($record.appId)', '$($record.appDisplayName)', '$($record.ipAddress)', '$($record.clientAppUsed)', '$($record.userAgent)', '$($record.correlationId)', '$($record.conditionalAccessStatus)', '$($record.originalRequestId)', $([int]$record.isInteractive), '$($record.tokenIssuerName)', '$($record.tokenIssuerType)', '$($record.clientCredentialType)', $([int]$record.processingTimeInMilliseconds), '$($record.riskDetail)', '$($record.riskLevelAggregated)', '$($record.riskLevelDuringSignIn)', '$($record.riskState)', '$($record.resourceDisplayName)', '$($record.resourceId)', '$($record.resourceTenantId)', '$($record.homeTenantId)', '$($record.homeTenantName)', '$($record.authenticationRequirement)', '$($record.signInIdentifier)', '$($record.signInIdentifierType)', '$($record.servicePrincipalName)', '$($record.userType)', $([int]$record.flaggedForReview), $([int]$record.isTenantRestricted), $([int]$record.autonomousSystemNumber), '$($record.crossTenantAccessType)', '$($record.uniqueTokenIdentifier)', '$($record.incomingTokenType)', '$($record.authenticationProtocol)', '$($record.signInTokenProtectionStatus)', '$($record.originalTransferMethod)', $([int]$record.isThroughGlobalSecureAccess), '$($record.globalSecureAccessIpAddress)', '$($record.sessionId)', '$($record.appOwnerTenantId)', '$($record.resourceOwnerTenantId)', $([int]$record.status.errorCode), '$failureReason', '$($record.status.additionalDetails)', '$($record.deviceDetail.deviceId)', '$($record.deviceDetail.displayName)', '$($record.deviceDetail.operatingSystem)', '$($record.deviceDetail.browser)', $([int]$record.deviceDetail.isCompliant), $([int]$record.deviceDetail.isManaged), '$($record.deviceDetail.trustType)', '$($record.location.city)', '$($record.location.state)', '$($record.location.countryOrRegion)', $([double]$record.location.geoCoordinates.latitude), $([double]$record.location.geoCoordinates.longitude)
                );
"@
                try {
                    Invoke-SqliteQuery -DataSource $dbPath -Query $query
                } catch {
                    Log-Error "Failed to insert record: $_"
                    Log-Error "Query: $query"
                }
            }
        }
    }
} finally {
    # Ensure the connection is closed
    if ($connection) {
        $connection.Close()
    }
}