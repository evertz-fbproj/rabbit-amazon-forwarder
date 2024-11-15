package pilot

import (
	"testing"
)

func TestToRequestJSON(t *testing.T) {
	t.Run("REQ_ADDED", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-253-109-Turbine','REQ_ADDED','1240358122','SV11232-TX','S3_Staging_Subtitles','','Calculated','','ip-10-238-253-109'%`
		out, err := ToRequestJSON(msg)
		if err != nil {
			t.Error(err)
		}
		if out == nil {
			t.Error("Output was nil", out)
		}
	})

	t.Run("REQ_UPDATED", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-253-109-Turbine','REQ_UPDATED','1240358122','SV11232-TX','S3_Staging_Subtitles','','Calculated','','ip-10-238-253-109'%`
		out, err := ToRequestJSON(msg)
		if err != nil {
			t.Error(err)
		}
		if out == nil {
			t.Error("Output was nil", out)
		}
	})

	t.Run("TRAN_UPDATED", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-205-Turbine','TRAN_UPDATED','1240358122','90196882','S3_Wrapped','S3_Staging_Subtitles','Copying','ip-10-238-252-205'%`
		out, err := ToRequestJSON(msg)
		if err != nil {
			t.Error(err)
		}
		if out == nil {
			t.Error("Output was nil", out)
		}
	})

	t.Run("TRAN_PROGRESS", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-205-Turbine','TRAN_PROGRESS','1240358122','90196882','5','Copying','ip-10-238-252-205'%`
		out, err := ToRequestJSON(msg)
		if err != nil {
			t.Error(err)
		}
		if out == nil {
			t.Error("Output was nil", out)
		}
	})

	t.Run("REQ_DELETED", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','REQ_DELETED','1240358122','SV11232-TX','S3_Staging_Subtitles','','In error','','ip-10-238-252-171'%`
		out, err := ToRequestJSON(msg)
		if err != nil {
			t.Error(err)
		}
		if out == nil {
			t.Error("Output was nil", out)
		}
	})

	t.Run("multiple messages in one", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','REQ_UPDATED','1240358122','SV11232-TX','S3_Staging_Subtitles','','Calculated','','ip-10-238-252-171'%%BRONOT 'REQUEST','ip-10-238-252-205-Turbine','TRAN_UPDATED','1240358122','90196883','S3_Wrapped','S3_Staging_Subtitles','Copying','ip-10-238-252-205'%%BRONOT 'REQUEST','ip-10-238-252-205-Turbine','TRAN_PROGRESS','1240358122','90196883','5','Copying','ip-10-238-252-205'%`
		out, err := ToRequestJSON(msg)
		if err != nil {
			t.Error(err)
		}
		if out == nil {
			t.Error("Output was nil", out)
		}
	})

	t.Run("not a pilot message", func(t *testing.T) {
		out, err := ToRequestJSON("something")
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("empty pilot message", func(t *testing.T) {
		out, err := ToRequestJSON("%BRONOT %")
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("not a request notification", func(t *testing.T) {
		out, err := ToRequestJSON(`%BRONOT 'OTHER','ip-10-238-252-171-Turbine','SOMETHINGELSE','1240358122','SV11232-TX','S3_Staging_Subtitles','','Calculated','','ip-10-238-252-171'%`)
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("malformed request notification - unknown type", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','SOMETHINGELSE','1240358122','SV11232-TX','S3_Staging_Subtitles','','Calculated','','ip-10-238-252-171'%`
		out, err := ToRequestJSON(msg)
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("malformed REQ_UPDATED - incorrect number of fields", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','REQ_UPDATED'%`
		out, err := ToRequestJSON(msg)
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("malformed REQ_DELETED - incorrect number of fields", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','REQ_DELETED'%`
		out, err := ToRequestJSON(msg)
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("malformed REQ_ADDED - incorrect number of fields", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','REQ_ADDED'%`
		out, err := ToRequestJSON(msg)
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("malformed TRAN_UPDATED - incorrect number of fields", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','TRAN_UPDATED'%`
		out, err := ToRequestJSON(msg)
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})

	t.Run("malformed TRAN_PROGRESS - incorrect number of fields", func(t *testing.T) {
		msg := `%BRONOT 'REQUEST','ip-10-238-252-171-Turbine','TRAN_PROGRESS'%`
		out, err := ToRequestJSON(msg)
		if err == nil {
			t.Error("Error was nil")
		}
		if out != nil {
			t.Error("Output was not nil", out)
		}
	})
}
