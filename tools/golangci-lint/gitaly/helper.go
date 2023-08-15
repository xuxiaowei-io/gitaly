package main

import (
	"fmt"
	"go/importer"
	"go/types"
)

func mustFindBuiltinInterface(name string) *types.Interface {
	obj := types.Universe.Lookup(name)
	if obj == nil {
		panic(fmt.Sprintf("fail to find universe interface %q", name))
	}
	return obj.Type().Underlying().(*types.Interface)
}

func mustFindPackageInterface(pkgName string, name string) *types.Interface {
	pkg, err := importer.Default().Import(pkgName)
	if err != nil {
		panic(fmt.Sprintf("fail to find package %q", pkgName))
	}
	obj := pkg.Scope().Lookup(name)
	if obj == nil {
		panic(fmt.Sprintf("fail to find universe interface %q of package %q", name, pkgName))
	}
	return obj.Type().Underlying().(*types.Interface)
}
