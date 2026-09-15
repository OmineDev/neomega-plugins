// Export the shared product defaults for the Python transport adapter.
package main

import (
 "encoding/json"
 "os"
 "github.com/maoqijie/Fatalder/shared/flow"
)
func main() {
 raw, err := json.Marshal(flow.DefaultBuildProfileIntent()); if err != nil { panic(err) }
 var values map[string]interface{}
 if err := json.Unmarshal(raw, &values); err != nil { panic(err) }
 d := flow.DefaultBuildDefaults()
 values["convert_to_mcworld"] = d.ConvertToMCWorld
 values["start_progress"] = d.StartProgress
 values["progress_mode"] = d.ProgressMode
 values["show_nbt_progress"] = d.ShowNBTProgress
 values["console_world_position"] = map[string]int{"x":d.ConsoleWorldX,"y":d.ConsoleWorldY,"z":d.ConsoleWorldZ}
 enc := json.NewEncoder(os.Stdout); enc.SetIndent("", "  "); if err := enc.Encode(values); err != nil { panic(err) }
}
