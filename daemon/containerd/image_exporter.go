package containerd

import (
	"context"
	"io"

	"github.com/containerd/containerd"
	cerrdefs "github.com/containerd/containerd/errdefs"
	containerdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/images/archive"
	"github.com/containerd/containerd/images/converter"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/container"
	"github.com/google/uuid"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func (i *ImageService) PerformWithBaseFS(ctx context.Context, c *container.Container, fn func(root string) error) error {
	snapshotter := i.client.SnapshotService(i.snapshotter)
	mounts, err := snapshotter.Mounts(ctx, c.ID)
	if err != nil {
		return err
	}
	return mount.WithTempMount(ctx, mounts, fn)
}

// ExportImage exports a list of images to the given output stream. The
// exported images are archived into a tar when written to the output
// stream. All images with the given tag and all versions containing
// the same tag are exported. names is the set of tags to export, and
// outStream is the writer which the images are written to.
//
// TODO(thaJeztah): produce JSON stream progress response and image events; see https://github.com/moby/moby/issues/43910
func (i *ImageService) ExportImage(ctx context.Context, names []string, outStream io.Writer) error {
	opts := []archive.ExportOpt{
		archive.WithSkipNonDistributableBlobs(),

		// This makes the exported archive also include `manifest.json`
		// when the image is a manifest list. It is needed for backwards
		// compatibility with Docker image format.
		// The containerd will choose only one manifest for the `manifest.json`.
		// Our preference is to have it point to the default platform.
		// Example:
		//  Daemon is running on linux/arm64
		//  When we export linux/amd64 and linux/arm64, manifest.json will point to linux/arm64.
		//  When we export linux/amd64 only, manifest.json will point to linux/amd64.
		// Note: This is only applicable if importing this archive into non-containerd Docker.
		// Importing the same archive into containerd, will not restrict the platforms.
		archive.WithPlatform(allPlatformsWithPreference(platforms.Default())),
	}

	for _, imageRef := range names {
		newOpt, tmpImage, err := i.optForImageExport(ctx, imageRef)
		if tmpImage != nil {
			defer i.client.ImageService().Delete(ctx, tmpImage.Name, containerdimages.SynchronousDelete())
		}
		if err != nil {
			return err
		}
		if newOpt != nil {
			opts = append(opts, newOpt)
		}
	}

	return i.client.Export(ctx, outStream, opts...)
}

// LoadImage uploads a set of images into the repository. This is the
// complement of ExportImage.  The input stream is an uncompressed tar
// ball containing images and metadata.
//
// TODO(thaJeztah): produce JSON stream progress response and image events; see https://github.com/moby/moby/issues/43910
func (i *ImageService) LoadImage(ctx context.Context, inTar io.ReadCloser, outStream io.Writer, quiet bool) error {
	platform := platforms.All
	imgs, err := i.client.Import(ctx, inTar, containerd.WithImportPlatform(platform))

	if err != nil {
		// TODO(thaJeztah): remove this log or change to debug once we can; see https://github.com/moby/moby/pull/43822#discussion_r937502405
		logrus.WithError(err).Warn("failed to import image to containerd")
		return errors.Wrap(err, "failed to import image")
	}

	for _, img := range imgs {
		platformImg := containerd.NewImageWithPlatform(i.client, img, platform)

		unpacked, err := platformImg.IsUnpacked(ctx, i.snapshotter)
		if err != nil {
			// TODO(thaJeztah): remove this log or change to debug once we can; see https://github.com/moby/moby/pull/43822#discussion_r937502405
			logrus.WithError(err).WithField("image", img.Name).Debug("failed to check if image is unpacked")
			continue
		}

		if !unpacked {
			err := platformImg.Unpack(ctx, i.snapshotter)
			if err != nil {
				// TODO(thaJeztah): remove this log or change to debug once we can; see https://github.com/moby/moby/pull/43822#discussion_r937502405
				logrus.WithError(err).WithField("image", img.Name).Warn("failed to unpack image")
				return errors.Wrap(err, "failed to unpack image")
			}
		}
	}
	return nil
}

// optForImageExport returns an archive.ExportOpt that should include the image
// with the provided name in the output archive.
func (i *ImageService) optForImageExport(ctx context.Context, name string) (archive.ExportOpt, *containerdimages.Image, error) {
	img, err := i.resolveImage(ctx, name, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to resolve image")
	}

	ref, err := reference.ParseNamed(img.Name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to parse image reference")
	}

	is := i.client.ImageService()
	store := i.client.ContentStore()

	if containerdimages.IsIndexType(img.Target.MediaType) {
		children, err := containerdimages.Children(ctx, store, img.Target)
		if err != nil {
			return nil, nil, err
		}

		// Check which platform manifests we have blobs for.
		missingPlatforms := []v1.Platform{}
		presentPlatforms := []v1.Platform{}
		for _, child := range children {
			if containerdimages.IsManifestType(child.MediaType) {
				_, err := store.ReaderAt(ctx, child)
				if cerrdefs.IsNotFound(err) {
					missingPlatforms = append(missingPlatforms, *child.Platform)
					logrus.WithField("digest", child.Digest.String()).Debug("missing blob, not exporting")
					continue
				} else if err != nil {
					return nil, nil, err
				}
				presentPlatforms = append(presentPlatforms, *child.Platform)
			}
		}

		// If we have all the manifests, just export the original index.
		if len(missingPlatforms) == 0 {
			return archive.WithImage(is, img.Name), nil, nil
		}

		// Create a new manifest which contains only the manifests we have in store.
		srcRef := ref.String()
		targetRef := srcRef + "-tmp-export" + uuid.NewString()
		newImg, err := converter.Convert(ctx, i.client, targetRef, srcRef,
			converter.WithPlatform(platforms.Any(presentPlatforms...)))
		if err != nil {
			return nil, newImg, err
		}
		return archive.WithManifest(newImg.Target, srcRef), newImg, nil
	}

	return archive.WithImage(is, img.Name), nil, nil
}

// allPlatformsWithPreference will match all platforms but will order
// platforms matching the preferred matcher first.
type allPlatformsWithPreferenceMatcher struct {
	preferred platforms.MatchComparer
}

func allPlatformsWithPreference(preferred platforms.MatchComparer) platforms.MatchComparer {
	return allPlatformsWithPreferenceMatcher{
		preferred: preferred,
	}
}

func (c allPlatformsWithPreferenceMatcher) Match(_ ocispec.Platform) bool {
	return true
}

func (c allPlatformsWithPreferenceMatcher) Less(p1, p2 ocispec.Platform) bool {
	return c.preferred.Less(p1, p2)
}
