// Package bundleuri is used to enable the use [Bundle-URI] when the client
// clones/fetches from the repository.
//
// Bundle-URI is a concept in Git that allows the server to send one or more
// URIs where [git bundles] are available. The client can download such bundles
// to prepopulate the repository before it starts the object negotiation with
// the server. This reduces the CPU load on the server, and the amount of
// traffic that has to travel directly from server to client.
//
// This feature piggy-backs onto server-side backups. Refer to the
// [backup documentation] how to create and store bundles on a cloud provider.
//
// [Bundle-URI]: https://git-scm.com/docs/bundle-uri
// [git bundles]: https://git-scm.com/docs/git-bundle
// [backup documentation]: https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/gitaly-backup.md
package bundleuri
