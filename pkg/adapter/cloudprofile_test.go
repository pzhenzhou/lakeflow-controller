package adapter

import "github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"

func testAliyunProfile() cloudprofile.CloudProfile {
	profile, _ := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	return profile
}
