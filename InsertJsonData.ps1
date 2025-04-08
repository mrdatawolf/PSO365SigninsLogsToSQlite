if (-not (Get-Module -ListAvailable -Name PSSQLite)) { 
    Install-Module -Name PSSQLite -Scope CurrentUser
}
Import-Module PSSQLite
# These are the variables that are used in the script
$primaryDirectory = "E:\Projects\Data\O365 Signins"
$databaseName = "O365logins.sqlite3"
$tableNames = @("Interactive", "NonInteractive")
$batchSize = 1000
#you shouldn't need change anythign below here.

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
    Write-ErrorLog "Failed to create database and table: $_"
}
}

function Write-ErrorLog {
param (
    [string]$errorMessage
)
$timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
$logMessage = "$timestamp - $errorMessage"
Add-Content -Path $logFilePath -Value $logMessage
}

foreach($tableName in $tableNames) {
    Write-Host "Proccessing table: $tableName"
    # Check if the table name is valid
    if ($tableName -notmatch '^[a-zA-Z0-9_]+$') {
        Write-Host "Invalid table name: $tableName. Skipping."
        continue
    }
    $jsonFolderPath = Join-Path $primaryDirectory $tableName
    $mainFinishedFolderPath = Join-Path $primaryDirectory "Finished"
    $finishedFolderPath = Join-Path $mainFinishedFolderPath $tableName
    $dbDirectory = $primaryDirectory

    # Ensure directories exist
    foreach ($path in @($mainFinishedFolderPath, $finishedFolderPath, $dbDirectory)) {
        if (-not (Test-Path $path)) {
            New-Item -Path $path -ItemType Directory | Out-Null
        }
    }

    $dbPath = Join-Path $dbDirectory $databaseName
    $logFilePath = Join-Path $dbDirectory "debug_$tableName.log"
    Initialize-Database -dbPath $dbPath
    Initialize-Table -dbPath $dbPath -tableName $tableName
    $connection = New-Object System.Data.SQLite.SQLiteConnection("Data Source=$dbPath;Version=3;")
    $connection.Open()
    Invoke-SqliteQuery -Query "PRAGMA synchronous = OFF;" -Connection $connection
    Invoke-SqliteQuery -Query "PRAGMA journal_mode = MEMORY;" -Connection $connection
    Invoke-SqliteQuery -Query "PRAGMA busy_timeout = 100;" -Connection $connection
    $jsonFiles = Get-ChildItem -Path $jsonFolderPath -Filter *.json -Recurse
    #$transaction = $connection.BeginTransaction()

    try {
        foreach ($file in $jsonFiles) {
            #Write-Host "Processing file: $($file.FullName)"
            $jsonContent = Get-Content -Path $file.FullName -Raw | ConvertFrom-Json
            $totalRecords = $jsonContent.Count
            $currentRecord = 0
            foreach ($record in $jsonContent) {
                $currentRecord++
                #Write-Host "Processing record $currentRecord of $totalRecords"
                if ($currentRecord % 100 -eq 0) {
                    Write-Progress -Activity "Processing file: $($file.FullName)" -Status "Processing record $currentRecord of $totalRecords" -PercentComplete (($currentRecord / $totalRecords) * 100)
                }
                # Escape single quotes in the failureReason field
                $failureReason = $record.status.failureReason -replace "'", "''"
    
                # Check if the id already exists
                $checkQuery = "SELECT COUNT(*) FROM $tableName WHERE id = '$($record.id)';"
                try {
                    #Write-Host "Executing query: $checkQuery"
                    $exists = Invoke-SqliteQuery -DataSource $dbPath -Query $checkQuery -As SingleValue
                    #Write-Host "Query result: $exists"
                } catch {
                    Write-ErrorLog "Error executing query: $checkQuery"
                    Write-ErrorLog $_.Exception.Message
                }
                
                if ($exists -eq 0) {
                    $query = @"
                    INSERT INTO $tableName (
                        id, createdDateTime, userDisplayName, userPrincipalName, userId, appId, appDisplayName, ipAddress, clientAppUsed, userAgent, correlationId, conditionalAccessStatus, originalRequestId, isInteractive, tokenIssuerName, tokenIssuerType, clientCredentialType, processingTimeInMilliseconds, riskDetail, riskLevelAggregated, riskLevelDuringSignIn, riskState, resourceDisplayName, resourceId, resourceTenantId, homeTenantId, homeTenantName, authenticationRequirement, signInIdentifier, signInIdentifierType, servicePrincipalName, userType, flaggedForReview, isTenantRestricted, autonomousSystemNumber, crossTenantAccessType, uniqueTokenIdentifier, incomingTokenType, authenticationProtocol, signInTokenProtectionStatus, originalTransferMethod, isThroughGlobalSecureAccess, globalSecureAccessIpAddress, sessionId, appOwnerTenantId, resourceOwnerTenantId, status_errorCode, status_failureReason, status_additionalDetails, deviceDetail_deviceId, deviceDetail_displayName, deviceDetail_operatingSystem, deviceDetail_browser, deviceDetail_isCompliant, deviceDetail_isManaged, deviceDetail_trustType, location_city, location_state, location_countryOrRegion, location_geoCoordinates_latitude, location_geoCoordinates_longitude
                    ) VALUES (
                        '$($record.id)', '$($record.createdDateTime)', '$($record.userDisplayName)', '$($record.userPrincipalName)', '$($record.userId)', '$($record.appId)', '$($record.appDisplayName)', '$($record.ipAddress)', '$($record.clientAppUsed)', '$($record.userAgent)', '$($record.correlationId)', '$($record.conditionalAccessStatus)', '$($record.originalRequestId)', $([int]$record.isInteractive), '$($record.tokenIssuerName)', '$($record.tokenIssuerType)', '$($record.clientCredentialType)', $([int]$record.processingTimeInMilliseconds), '$($record.riskDetail)', '$($record.riskLevelAggregated)', '$($record.riskLevelDuringSignIn)', '$($record.riskState)', '$($record.resourceDisplayName)', '$($record.resourceId)', '$($record.resourceTenantId)', '$($record.homeTenantId)', '$($record.homeTenantName)', '$($record.authenticationRequirement)', '$($record.signInIdentifier)', '$($record.signInIdentifierType)', '$($record.servicePrincipalName)', '$($record.userType)', $([int]$record.flaggedForReview), $([int]$record.isTenantRestricted), $([int]$record.autonomousSystemNumber), '$($record.crossTenantAccessType)', '$($record.uniqueTokenIdentifier)', '$($record.incomingTokenType)', '$($record.authenticationProtocol)', '$($record.signInTokenProtectionStatus)', '$($record.originalTransferMethod)', $([int]$record.isThroughGlobalSecureAccess), '$($record.globalSecureAccessIpAddress)', '$($record.sessionId)', '$($record.appOwnerTenantId)', '$($record.resourceOwnerTenantId)', $([int]$record.status.errorCode), '$failureReason', '$($record.status.additionalDetails)', '$($record.deviceDetail.deviceId)', '$($record.deviceDetail.displayName)', '$($record.deviceDetail.operatingSystem)', '$($record.deviceDetail.browser)', $([int]$record.deviceDetail.isCompliant), $([int]$record.deviceDetail.isManaged), '$($record.deviceDetail.trustType)', '$($record.location.city)', '$($record.location.state)', '$($record.location.countryOrRegion)', $([double]$record.location.geoCoordinates.latitude), $([double]$record.location.geoCoordinates.longitude)
                    );
"@
                    try {
                        #Write-Host "Executing insert query: $query"
                        Invoke-SqliteQuery -DataSource $dbPath -Query $query
                        #Write-Host "Insert successful"
                    } catch {
                        #Write-Host "Error executing insert query: $query"
                        #Write-Host $_.Exception.Message
                        Write-ErrorLog "Failed to insert record: $_"
                        Write-ErrorLog "Query: $query"
                    }
                }
            }
            # Determine the relative path of the file within $jsonFolderPath
            $relativePath = $file.FullName.Substring($jsonFolderPath.Length + 1)
            $destinationPath = Join-Path $finishedFolderPath $relativePath
    
            # Ensure the destination directory exists
            $destinationDirectory = Split-Path $destinationPath -Parent
            if (-not (Test-Path $destinationDirectory)) {
                New-Item -Path $destinationDirectory -ItemType Directory
            }
    
            # Move the file to the corresponding subfolder in $finishedFolderPath
            Move-Item -Path $file.FullName -Destination $destinationPath
        }
    } catch {
        Write-Host "Error occurred: $_"
        Write-ErrorLog "Transaction failed: $_"
    } finally {
        Write-Host "Closing database connection"
        if ($connection) {
            $connection.Close()
        }
    }
}