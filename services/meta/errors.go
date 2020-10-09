package meta

import (
	"errors"
)

var (
	// ErrNodeExists is returned when creating an already existing node.
	ErrNodeExists = errors.New("node already exists")

	// ErrNodeNotFound is returned when mutating a node that doesn't exist.
	ErrNodeNotFound = errors.New("node not found")

	// ErrNodeAlreadyFreezed represents node has been already freezed
	ErrNodeAlreadyFreezed = errors.New("node has been freezed before")

	// ErrNodeNotFreezed represents node is not freezed (is normal)
	ErrNodeNotFreezed = errors.New("node hasn't been freezed")

	// ErrNodesRequired is returned when at least one node is required for an operation.
	// This occurs when creating a shard group.
	ErrNodesRequired = errors.New("at least one node required")

	// ErrNodeIDRequired is returned when using a zero node id.
	ErrNodeIDRequired = errors.New("node id must be greater than 0")

	// ErrNodeUnableToDropFinalNode is returned if the node being dropped is the last
	// node in the cluster
	ErrNodeUnableToDropFinalNode = errors.New("unable to drop the final node in a cluster")
)
