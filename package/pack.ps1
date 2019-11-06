Param(
  [parameter(Mandatory=$false)][string]$repo="http://192.168.0.19:8081/repository/nuget-hosted/",
  [parameter(Mandatory=$false)][bool]$push=$false,
  [parameter(Mandatory=$false)][string]$apikey,
  [parameter(Mandatory=$false)][bool]$build=$true
)

# Paths
$packFolder = (Get-Item -Path "./" -Verbose).FullName
$slnPath = Join-Path $packFolder "../"
$srcPath = Join-Path $packFolder "../src/"



$projects = (Get-Content "./Components")

function Pack($projectFolder,$projectName) {  
  Set-Location $projectFolder
  $releaseProjectFolder = (Join-Path $projectFolder "bin/Release")
  if (Test-Path $releaseProjectFolder)
  {
     Remove-Item -Force -Recurse $releaseProjectFolder
  }
  
   & dotnet msbuild /p:Configuration=Release /p:SourceLinkCreate=true
   & dotnet msbuild /t:pack /p:Configuration=Release /p:SourceLinkCreate=true
   if ($projectName) {
    $projectPackPath = Join-Path $projectFolder ("/bin/Release/" + $projectName + ".*.nupkg")
   }else {
    $projectPackPath = Join-Path $projectFolder ("/bin/Release/" + $project + ".*.nupkg")
   }
   Move-Item -Force $projectPackPath $packFolder 
}

if ($build) {
  Set-Location $slnPath
  & dotnet restore Ray.sln

  foreach($project in $projects) {
    if (-not $project.StartsWith("#")){
      Pack -projectFolder (Join-Path $srcPath $project)
    }    
  }
  
  Set-Location $packFolder
}

if($push) {
    if ([string]::IsNullOrEmpty($apikey)){
        Write-Warning -Message "未设置nuget仓库的APIKEY"
		exit 1
	}
	dotnet nuget push *.nupkg -s $repo -k $apikey
}