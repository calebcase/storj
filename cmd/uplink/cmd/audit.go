// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"storj.io/common/fpath"
	"storj.io/storj/pkg/process"
	"storj.io/storj/pkg/storj"
)

func init() {
	addCmd(&cobra.Command{
		Use:   "audit",
		Short: "Audit segments and nodes",
		RunE:  auditMain,
	}, RootCmd)
}

func audit(ctx context.Context, src fpath.FPath) (err error) {
	project, bucket, err := cfg.GetProjectAndBucket(ctx, src.Bucket())
	if err != nil {
		return err
	}
	defer closeProjectAndBucket(project, bucket)

	object, err := bucket.OpenObject(ctx, src.Path())
	if err != nil {
		return convertError(err, src)
	}

	ros, err := object.GetSegmentStream(ctx, 0, object.Meta.Size)
	if err != nil {
		return err
	}

	var segments []storj.Segment
	var more bool = true
	for more {
		segments, more, err = ros.Segments(ctx, 0, 1024)
		if err != nil {
			return err
		}

		for _, segment := range segments {
			for _, piece := range segment.Pieces {
				fmt.Printf("%s\n", piece.Location)
			}
		}
	}

	return nil
}

// auditMain is the function executed when auditCmd is called.
func auditMain(cmd *cobra.Command, args []string) (err error) {
	if len(args) == 0 {
		return fmt.Errorf("no object specified for copy")
	}

	ctx, _ := process.Ctx(cmd)

	src, err := fpath.New(args[0])
	if err != nil {
		return err
	}

	return audit(ctx, src)
}
