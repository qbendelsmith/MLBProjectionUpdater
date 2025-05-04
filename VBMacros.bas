Attribute VB_Name = "Module1"

Function GetOpponent(teamCode As String) As String
    Dim probablesSheet As Worksheet
    Dim i As Integer
    Dim lastRow As Integer
    
    ' Set reference to the probables sheet
    On Error Resume Next
    Set probablesSheet = ThisWorkbook.Worksheets("probables")
    On Error GoTo 0
    
    ' Check if probables sheet exists
    If probablesSheet Is Nothing Then
        GetOpponent = "ERROR: Probables sheet not found"
        Exit Function
    End If
    
    ' Find the last row with data
    lastRow = probablesSheet.Cells(probablesSheet.Rows.Count, "B").End(xlUp).Row
    
    ' Look for the team in Away column (Column B - index 2)
    For i = 2 To lastRow
        If probablesSheet.Cells(i, 2).Value = teamCode Then
            GetOpponent = probablesSheet.Cells(i, 7).Value
            Exit Function
        End If
    Next i
    
    ' Look for the team in Home column (Column C - index 3)
    For i = 2 To lastRow
        If probablesSheet.Cells(i, 3).Value = teamCode Then
            GetOpponent = probablesSheet.Cells(i, 6).Value
            Exit Function
        End If
    Next i
    
    ' No match found
    GetOpponent = "No Game Today"
End Function

Function GetOpposingPitcher(teamCode As String) As String
    Dim probablesSheet As Worksheet
    Dim i As Integer
    Dim lastRow As Integer
    
    ' Set reference to the probables sheet
    On Error Resume Next
    Set probablesSheet = ThisWorkbook.Worksheets("probables")
    On Error GoTo 0
    
    ' Check if probables sheet exists
    If probablesSheet Is Nothing Then
        GetOpposingPitcher = "ERROR: Probables sheet not found"
        Exit Function
    End If
    
    ' Find the last row with data
    lastRow = probablesSheet.Cells(probablesSheet.Rows.Count, "B").End(xlUp).Row
    
    ' Look for the team in Away column (Column B - index 2)
    For i = 2 To lastRow
        If probablesSheet.Cells(i, 2).Value = teamCode Then
            GetOpposingPitcher = probablesSheet.Cells(i, 5).Value
            Exit Function
        End If
    Next i
    
    ' Look for the team in Home column (Column C - index 3)
    For i = 2 To lastRow
        If probablesSheet.Cells(i, 3).Value = teamCode Then
            GetOpposingPitcher = probablesSheet.Cells(i, 4).Value
            Exit Function
        End If
    Next i
    
    ' No match found
    GetOpposingPitcher = "No Pitcher"
End Function

Function GetHomeTeamForMatchup(teamCode As String, probablesRange As Range) As Variant
    ' Simple function to get the home team in a matchup involving the given team
    ' If the team is the home team, returns the team itself
    ' If the team is the away team, returns the opponent
    
    Dim i As Long
    Dim headers As Range
    Dim dataRows As Range
    Dim homeTeamIndex As Integer
    Dim awayTeamIndex As Integer
    
    ' Set up headers and data rows
    Set headers = probablesRange.Rows(1)
    Set dataRows = probablesRange.Offset(1, 0).Resize(probablesRange.Rows.Count - 1)
    
    ' Find column indexes
    homeTeamIndex = -1
    awayTeamIndex = -1
    
    For i = 1 To headers.Columns.Count
        Select Case headers.Cells(1, i).Value
            Case "HomeTeam"
                homeTeamIndex = i
            Case "AwayTeam"
                awayTeamIndex = i
        End Select
    Next i
    
    ' Validate column indexes
    If homeTeamIndex = -1 Or awayTeamIndex = -1 Then
        GetHomeTeamForMatchup = "Columns not found"
        Exit Function
    End If
    
    ' Check if the team is playing as home team
    For i = 1 To dataRows.Rows.Count
        If dataRows.Cells(i, homeTeamIndex).Value = teamCode Then
            ' The team is the home team, return itself
            GetHomeTeamForMatchup = teamCode
            Exit Function
        End If
    Next i
    
    ' If not home team, search as away team and return the opponent (home team)
    For i = 1 To dataRows.Rows.Count
        If dataRows.Cells(i, awayTeamIndex).Value = teamCode Then
            ' The team is the away team, return the opponent (home team)
            GetHomeTeamForMatchup = dataRows.Cells(i, homeTeamIndex).Value
            Exit Function
        End If
    Next i
    
    ' If team not found in either column, return default value
    GetHomeTeamForMatchup = teamCode  ' Default to the team itself if not found
End Function

Function PlayerProjection(l3yrs As Variant, curr As Variant, l7 As Variant, pitcherWeight As Variant, parkFactor As Variant) As Double
    ' Set default values for unknown pitchers (significantly below average)
    Dim defaultPitchWeight As Double
    defaultPitchWeight = 7.5
    
    Dim avgPitchWeight As Double
    avgPitchWeight = Application.WorksheetFunction.Average(Sheets("Pitcher").Range("AP3:AP1000"))
    
    ' Calculate base projection with dynamic weights
    Dim l3yrsWeight As Double, currWeight As Double, l7Weight As Double
    Dim totalWeight As Double, baseProj As Double
    
    ' Assign weights based on available data
    l3yrsWeight = IIf(IsError(l3yrs) Or IsEmpty(l3yrs), 0, 0.4)
    currWeight = IIf(IsError(curr) Or IsEmpty(curr), 0, 0.4)
    l7Weight = IIf(IsError(l7) Or IsEmpty(l7), 0, 0.2)
    
    ' Calculate total weight
    totalWeight = l3yrsWeight + currWeight + l7Weight
    If totalWeight = 0 Then
        PlayerProjection = 0
        Exit Function
    End If
    
    ' Normalize weights if some data is missing
    l3yrsWeight = l3yrsWeight / totalWeight
    currWeight = currWeight / totalWeight
    l7Weight = l7Weight / totalWeight
    
    ' Calculate base projection
    baseProj = 0
    If Not (IsError(l3yrs) Or IsEmpty(l3yrs)) Then baseProj = baseProj + l3yrs * l3yrsWeight
    If Not (IsError(curr) Or IsEmpty(curr)) Then baseProj = baseProj + curr * currWeight
    If Not (IsError(l7) Or IsEmpty(l7)) Then baseProj = baseProj + l7 * l7Weight
    
    ' Park factor adjustment
    Dim parkAdj As Double
    parkAdj = IIf(IsError(parkFactor) Or IsEmpty(parkFactor), 1, parkFactor / 100)
    
    ' Pitcher weight adjustment - use 7.5 for unknown pitchers
    Dim pitcherAdj As Double, pitchWeightVal As Double
    pitchWeightVal = IIf(IsError(pitcherWeight) Or IsEmpty(pitcherWeight), defaultPitchWeight, pitcherWeight)
    pitcherAdj = 1 - ((pitchWeightVal / avgPitchWeight) - 1) * 0.15
    
    ' Final projection
    PlayerProjection = baseProj * parkAdj * pitcherAdj
    
    ' Ensure projection is non-negative
    If PlayerProjection < 0 Then PlayerProjection = 0
End Function

' Function to get offensive strength from team batting stats
Function GetTeamOffenseStrength(teamCode As String) As Double
    Dim tmHittingSheet As Worksheet
    Dim i As Integer
    Dim lastRow As Integer
    
    ' Default value if team not found
    GetTeamOffenseStrength = 0
    
    ' Set reference to the team hitting sheet
    On Error Resume Next
    Set tmHittingSheet = ThisWorkbook.Worksheets("FGTmHitting")
    On Error GoTo 0
    
    ' Check if team hitting sheet exists
    If tmHittingSheet Is Nothing Then
        Exit Function
    End If
    
    ' Find the last row with data
    lastRow = tmHittingSheet.Cells(tmHittingSheet.Rows.Count, "A").End(xlUp).Row
    
    ' Look for the team code in TeamCode column (assuming this is column A)
    For i = 2 To lastRow  ' Assuming row 1 has headers
        If tmHittingSheet.Cells(i, 1).Value = teamCode Then
            ' Find the OffenseScore column (assuming this is column with "OffenseScore" header)
            Dim scoreCol As Integer
            Dim colIdx As Integer
            
            ' Find the column with OffenseScore header
            For colIdx = 1 To tmHittingSheet.Cells(1, tmHittingSheet.Columns.Count).End(xlToLeft).Column
                If tmHittingSheet.Cells(1, colIdx).Value = "OffenseScore" Then
                    scoreCol = colIdx
                    Exit For
                End If
            Next colIdx
            
            ' If found, return the value
            If scoreCol > 0 Then
                GetTeamOffenseStrength = tmHittingSheet.Cells(i, scoreCol).Value
            End If
            
            Exit Function
        End If
    Next i
End Function

' Function to calculate opponent weight based on offense strength
Function GetOpponentWeight(teamCode As String) As Double
    Dim offenseScore As Double
    Dim minMultiplier As Double
    Dim maxMultiplier As Double
    
    ' Default multiplier if calculation fails
    GetOpponentWeight = 1
    
    ' Set min and max multiplier values (same as in Python code)
    minMultiplier = 0.7
    maxMultiplier = 1.3
    
    ' Get team offense strength
    offenseScore = GetTeamOffenseStrength(teamCode)
    
    ' If no valid score found, return default
    If offenseScore = 0 Then
        Exit Function
    End If
    
    ' Calculate the multiplier using all teams in the sheet
    Dim tmHittingSheet As Worksheet
    Dim lastRow As Integer
    Dim scoreCol As Integer
    Dim i As Integer
    Dim scores() As Double
    Dim scoreCount As Integer
    
    ' Get the team hitting sheet
    On Error Resume Next
    Set tmHittingSheet = ThisWorkbook.Worksheets("FGTmHitting")
    On Error GoTo 0
    
    If tmHittingSheet Is Nothing Then
        Exit Function
    End If
    
    ' Find the OffenseScore column
    For i = 1 To tmHittingSheet.Cells(1, tmHittingSheet.Columns.Count).End(xlToLeft).Column
        If tmHittingSheet.Cells(1, i).Value = "OffenseScore" Then
            scoreCol = i
            Exit For
        End If
    Next i
    
    ' If column not found, return default
    If scoreCol = 0 Then
        Exit Function
    End If
    
    ' Find the last row with data
    lastRow = tmHittingSheet.Cells(tmHittingSheet.Rows.Count, 1).End(xlUp).Row
    
    ' Collect all scores
    ReDim scores(1 To lastRow - 1)
    scoreCount = 0
    
    For i = 2 To lastRow
        If IsNumeric(tmHittingSheet.Cells(i, scoreCol).Value) Then
            scoreCount = scoreCount + 1
            scores(scoreCount) = tmHittingSheet.Cells(i, scoreCol).Value
        End If
    Next i
    
    ' Find min and max scores
    Dim minScore As Double
    Dim maxScore As Double
    Dim normalizedScore As Double
    Dim invertedScore As Double
    
    If scoreCount > 0 Then
        minScore = Application.Min(scores)
        maxScore = Application.Max(scores)
        
        ' Normalize and invert the score (1 - normalized)
        If maxScore > minScore Then
            normalizedScore = (offenseScore - minScore) / (maxScore - minScore)
            invertedScore = 1 - normalizedScore
            
            ' Scale to desired multiplier range
            GetOpponentWeight = minMultiplier + invertedScore * (maxMultiplier - minMultiplier)
        End If
    End If
End Function

Function CalculatePitcherProjection(l3yrs As Variant, curr As Variant, l30 As Variant, _
                                    offenseScore As Variant, park As Variant) As Double
    ' Calculate pitcher projection using the same parameters as PlayerProjection
    
    ' Set default values for missing data
    Dim defaultOffenseScore As Double
    defaultOffenseScore = 0  ' Average offense
    
    ' Calculate base projection with dynamic weights
    Dim l3yrsWeight As Double, currWeight As Double, l30Weight As Double
    Dim totalWeight As Double, baseProj As Double
    
    ' Assign weights based on available data
    l3yrsWeight = IIf(IsError(l3yrs) Or IsEmpty(l3yrs), 0, 0.4)
    currWeight = IIf(IsError(curr) Or IsEmpty(curr), 0, 0.4)
    l30Weight = IIf(IsError(l30) Or IsEmpty(l30), 0, 0.2)
    
    ' Calculate total weight
    totalWeight = l3yrsWeight + currWeight + l30Weight
    If totalWeight = 0 Then
        CalculatePitcherProjection = 0
        Exit Function
    End If
    
    ' Normalize weights if some data is missing
    l3yrsWeight = l3yrsWeight / totalWeight
    currWeight = currWeight / totalWeight
    l30Weight = l30Weight / totalWeight
    
    ' Calculate base projection
    baseProj = 0
    If Not (IsError(l3yrs) Or IsEmpty(l3yrs)) Then baseProj = baseProj + l3yrs * l3yrsWeight
    If Not (IsError(curr) Or IsEmpty(curr)) Then baseProj = baseProj + curr * currWeight
    If Not (IsError(l30) Or IsEmpty(l30)) Then baseProj = baseProj + l30 * l30Weight
    
    ' Park factor adjustment
    Dim parkAdj As Double
    parkAdj = IIf(IsError(park) Or IsEmpty(park), 1, park / 100)
    
    ' Convert offense score to a multiplier (higher offense score = worse for pitcher)
    ' Range: 0.7 (best hitters) to 1.3 (worst hitters)
    Dim offenseAdj As Double
    Dim offenseVal As Double
    
    offenseVal = IIf(IsError(offenseScore) Or IsEmpty(offenseScore), defaultOffenseScore, offenseScore)
    
    ' Convert from the -1.7 to 2.3 range to a 0.7 to 1.3 range
    ' Higher offensive teams (positive scores) decrease pitcher projections
    ' Lower offensive teams (negative scores) increase pitcher projections
    offenseAdj = 1 - (offenseVal * 0.15)
    
    ' Cap the adjustment between 0.7 and 1.3
    If offenseAdj < 0.7 Then
        offenseAdj = 0.7
    ElseIf offenseAdj > 1.3 Then
        offenseAdj = 1.3
    End If
    
    ' Final projection
    CalculatePitcherProjection = baseProj * parkAdj * offenseAdj
    
    ' Ensure projection is non-negative
    If CalculatePitcherProjection < 0 Then CalculatePitcherProjection = 0
End Function

' Function to filter only starting pitchers (with at least 1 GS)
Function IsStartingPitcher(pitcherName As String) As Boolean
    Dim pitcherSheet As Worksheet
    Dim i As Integer
    Dim lastRow As Integer
    
    ' Default result if not found
    IsStartingPitcher = False
    
    ' Set reference to the pitcher sheet
    On Error Resume Next
    Set pitcherSheet = ThisWorkbook.Worksheets("Pitcher")
    On Error GoTo 0
    
    ' Check if pitcher sheet exists
    If pitcherSheet Is Nothing Then
        Exit Function
    End If
    
    ' Find the last row with data
    lastRow = pitcherSheet.Cells(pitcherSheet.Rows.Count, "B").End(xlUp).Row  ' Assuming Name is in column B
    
    ' Look for the pitcher by name
    For i = 2 To lastRow  ' Assuming row 1 has headers
        If pitcherSheet.Cells(i, 2).Value = pitcherName Then  ' Column B (2) is Name
            ' Find columns containing GS (Games Started)
            Dim gsFound As Boolean
            gsFound = False
            
            ' Scan header row to find GS columns
            For colIdx = 1 To pitcherSheet.Cells(1, pitcherSheet.Columns.Count).End(xlToLeft).Column
                If InStr(1, pitcherSheet.Cells(1, colIdx).Value, "GS", vbTextCompare) > 0 Then
                    ' Check if this pitcher has at least 1 GS in this column
                    If IsNumeric(pitcherSheet.Cells(i, colIdx).Value) Then
                        If pitcherSheet.Cells(i, colIdx).Value >= 1 Then
                            IsStartingPitcher = True
                            Exit Function
                        End If
                    End If
                End If
            Next colIdx
            
            Exit Function
        End If
    Next i
End Function

Function VLOOKUP2(lookup_value1 As Variant, lookup_value2 As Variant, _
                  table_array As Range, col_index_num As Integer, _
                  name_col As Integer, team_col As Integer) As Variant
    Dim i As Long
    
    ' Loop through all rows in the table array
    For i = 1 To table_array.Rows.Count
        ' Check if both lookup values match
        If table_array.Cells(i, name_col).Value = lookup_value1 And _
           table_array.Cells(i, team_col).Value = lookup_value2 Then
            ' Return the corresponding value from the result column
            VLOOKUP2 = table_array.Cells(i, col_index_num).Value
            Exit Function
        End If
    Next i
    
    ' If no match is found, return #N/A
    VLOOKUP2 = CVErr(xlErrNA)
End Function