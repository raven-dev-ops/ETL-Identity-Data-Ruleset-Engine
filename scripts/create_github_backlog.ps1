Param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,

    [string]$BacklogPath = "planning/post-v0.1.0-github-issues-backlog.md",

    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-GhExecutable {
    $venvGh = Join-Path ".venv" "Scripts\gh.exe"
    if (Test-Path $venvGh) {
        return (Resolve-Path $venvGh).Path
    }

    $candidate = Get-Command "gh" -ErrorAction SilentlyContinue
    if ($candidate) {
        return $candidate.Source
    }

    throw "GitHub CLI not found. Run the bootstrap script to install the venv-scoped gh binary."
}

function Invoke-Gh {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    $output = & $script:GhExe @Args
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "gh command failed (exit $exitCode): gh $($Args -join ' ')"
    }
    return $output
}

function Get-SectionBody {
    param(
        [string]$Content,
        [string]$StartHeading,
        [string]$EndHeading
    )
    $pattern = "(?ms)^## $([regex]::Escape($StartHeading))\s*(?<body>.*?)(?=^## $([regex]::Escape($EndHeading))|\z)"
    $match = [regex]::Match($Content, $pattern)
    if (-not $match.Success) {
        return ""
    }
    return $match.Groups["body"].Value
}

function Get-LabelColor {
    param([string]$Label)
    switch -Wildcard ($Label) {
        "type:*" { return "5319E7" }
        "area:*" { return "0E8A16" }
        "priority:p0" { return "B60205" }
        "priority:p1" { return "FBCA04" }
        "priority:p2" { return "CCCCCC" }
        default { return "1D76DB" }
    }
}

function Get-LabelDescription {
    param([string]$Label)
    if ($Label.StartsWith("type:")) {
        return "Issue type label"
    }
    if ($Label.StartsWith("area:")) {
        return "Subsystem ownership label"
    }
    if ($Label.StartsWith("priority:")) {
        return "Delivery priority label"
    }
    return "Project label"
}

function Parse-Backlog {
    param([string]$BacklogText)

    $milestoneBody = Get-SectionBody -Content $BacklogText -StartHeading "Milestones" -EndHeading "Label Set To Create"
    $milestones = @(
        [regex]::Matches($milestoneBody, '-\s*`([^`]+)`') | ForEach-Object { $_.Groups[1].Value }
    ) | Select-Object -Unique

    $labelBody = Get-SectionBody -Content $BacklogText -StartHeading "Label Set To Create" -EndHeading "Issue Catalog"
    $labels = @(
        [regex]::Matches($labelBody, '-\s*`([^`]+)`') | ForEach-Object { $_.Groups[1].Value }
    ) | Select-Object -Unique

    $issuePattern = "(?ms)^###\s+\d+\)\s+(?<title>.+?)\r?\n\r?\n(?<body>.*?)(?=^###\s+\d+\)|^##\s+Suggested Epic Issues)"
    $issueMatches = [regex]::Matches($BacklogText, $issuePattern)
    $issues = @()

    foreach ($match in $issueMatches) {
        $title = $match.Groups["title"].Value.Trim()
        $body = $match.Groups["body"].Value

        $milestoneMatch = [regex]::Match($body, '-\s+Milestone:\s+`([^`]+)`')
        $labelsMatch = [regex]::Match($body, "-\s+Labels:\s+(.+)")
        $dependsMatch = [regex]::Match($body, "-\s+Depends on:\s+(.+)")

        $descriptionMatch = [regex]::Match($body, "(?ms)-\s+Description:\s*(?<text>.*?)(?=-\s+Acceptance criteria:)")
        $acceptanceMatch = [regex]::Match($body, "(?ms)-\s+Acceptance criteria:\s*(?<text>.*)")

        $issueLabels = @()
        if ($labelsMatch.Success) {
            $issueLabels = @(
                [regex]::Matches($labelsMatch.Groups[1].Value, '`([^`]+)`') | ForEach-Object { $_.Groups[1].Value }
            )
        }

        $descriptionItems = @()
        if ($descriptionMatch.Success) {
            $descriptionItems = @(
                [regex]::Matches($descriptionMatch.Groups["text"].Value, "^\s*-\s+(.+)$", "Multiline") | ForEach-Object { $_.Groups[1].Value.Trim() }
            )
        }

        $acceptanceItems = @()
        if ($acceptanceMatch.Success) {
            $acceptanceItems = @(
                [regex]::Matches($acceptanceMatch.Groups["text"].Value, "^\s*-\s+(.+)$", "Multiline") | ForEach-Object { $_.Groups[1].Value.Trim() }
            )
        }

        $issues += [pscustomobject]@{
            Title = $title
            Milestone = if ($milestoneMatch.Success) { $milestoneMatch.Groups[1].Value } else { "" }
            Labels = $issueLabels
            DependsOn = if ($dependsMatch.Success) { $dependsMatch.Groups[1].Value.Trim() } else { "none" }
            DescriptionItems = $descriptionItems
            AcceptanceItems = $acceptanceItems
        }
    }

    $epicBody = Get-SectionBody -Content $BacklogText -StartHeading "Suggested Epic Issues" -EndHeading "Suggested Issue Creation Order"
    $epicMatches = [regex]::Matches($epicBody, '^\d+\.\s+Epic:\s+(.+?)\s+\(`([^`]+)`\)', "Multiline")
    $epics = @()
    foreach ($epic in $epicMatches) {
        $epics += [pscustomobject]@{
            Title = $epic.Groups[1].Value.Trim()
            Milestone = $epic.Groups[2].Value.Trim()
            Labels = @("type:epic")
            DependsOn = "none"
            DescriptionItems = @("Epic created from $($BacklogPath.Replace('\', '/'))")
            AcceptanceItems = @("Child issues linked and tracked to completion.")
        }
    }

    return [pscustomobject]@{
        Milestones = $milestones
        Labels = $labels
        Epics = $epics
        Issues = $issues
    }
}

function Ensure-Labels {
    param(
        [string]$RepoName,
        [string[]]$LabelNames
    )

    foreach ($label in $LabelNames) {
        $args = @(
            "label", "create",
            "--repo", $RepoName,
            "--force",
            "--color", (Get-LabelColor -Label $label),
            "--description", (Get-LabelDescription -Label $label),
            $label
        )
        if ($DryRun) {
            Write-Host "[DRY-RUN] gh $($args -join ' ')"
        }
        else {
            Invoke-Gh @args | Out-Null
            Write-Host "label upserted: $label"
        }
    }
}

function Ensure-Milestones {
    param(
        [string]$RepoName,
        [string[]]$MilestoneNames
    )

    if ($DryRun) {
        foreach ($milestone in $MilestoneNames) {
            Write-Host "[DRY-RUN] gh api repos/$RepoName/milestones --method POST -f title='$milestone'"
        }
        return
    }

    $existing = Invoke-Gh api "repos/$RepoName/milestones?state=all&per_page=100" | ConvertFrom-Json
    $existingTitles = @($existing | ForEach-Object { $_.title })

    foreach ($milestone in $MilestoneNames) {
        if ($existingTitles -contains $milestone) {
            Write-Host "milestone exists: $milestone"
            continue
        }

        Invoke-Gh api "repos/$RepoName/milestones" --method POST -f "title=$milestone" | Out-Null
        Write-Host "milestone created: $milestone"
    }
}

function Ensure-Issues {
    param(
        [string]$RepoName,
        [object[]]$IssueItems
    )

    $existingTitles = @()
    if (-not $DryRun) {
        $existingIssues = Invoke-Gh issue list --repo $RepoName --state all --limit 500 --json title | ConvertFrom-Json
        $existingTitles = @($existingIssues | ForEach-Object { $_.title })
    }

    foreach ($issue in $IssueItems) {
        if ((-not $DryRun) -and ($existingTitles -contains $issue.Title)) {
            Write-Host "issue exists: $($issue.Title)"
            continue
        }

        $bodyLines = @(
            "## Milestone",
            "",
            ("- ``{0}``" -f $issue.Milestone),
            "",
            "## Depends On",
            "",
            "- $($issue.DependsOn)",
            "",
            "## Description"
        )
        foreach ($item in $issue.DescriptionItems) {
            $bodyLines += "- $item"
        }
        $bodyLines += ""
        $bodyLines += "## Acceptance Criteria"
        foreach ($item in $issue.AcceptanceItems) {
            $bodyLines += "- $item"
        }
        $bodyText = ($bodyLines -join "`n")

        $args = @(
            "issue", "create",
            "--repo", $RepoName,
            "--title", $issue.Title,
            "--body", $bodyText
        )
        if ($issue.Milestone) {
            $args += @("--milestone", $issue.Milestone)
        }
        foreach ($label in $issue.Labels) {
            $args += @("--label", $label)
        }

        if ($DryRun) {
            Write-Host "[DRY-RUN] gh $($args -join ' ')"
            continue
        }

        Invoke-Gh @args | Out-Null
        Write-Host "issue created: $($issue.Title)"
    }
}

if (-not $DryRun) {
    $script:GhExe = Resolve-GhExecutable
    Write-Host "using gh executable: $script:GhExe"
}

if (-not (Test-Path $BacklogPath)) {
    throw "Backlog file not found: $BacklogPath"
}

$backlogText = Get-Content -Raw -Path $BacklogPath
$parsed = Parse-Backlog -BacklogText $backlogText

Write-Host "parsed milestones: $($parsed.Milestones.Count)"
Write-Host "parsed labels: $($parsed.Labels.Count)"
Write-Host "parsed epics: $($parsed.Epics.Count)"
Write-Host "parsed issues: $($parsed.Issues.Count)"

Ensure-Labels -RepoName $Repo -LabelNames $parsed.Labels
Ensure-Milestones -RepoName $Repo -MilestoneNames $parsed.Milestones
Ensure-Issues -RepoName $Repo -IssueItems $parsed.Epics
Ensure-Issues -RepoName $Repo -IssueItems $parsed.Issues

Write-Host "backlog creation complete"
