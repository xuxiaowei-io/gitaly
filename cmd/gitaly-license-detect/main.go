package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")

	flag.Parse()

	licensedb.Preload()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// ... rest of the program ...

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Cfg{
		BinDir: "/usr/bin",
		Storages: []config.Storage{
			{
				Name: "default",
				Path: "/home",
			},
		},
	}
	cmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	catfileCache := catfile.NewCache(cfg)
	locator := config.NewLocator(cfg)
	repoProto := &gitalypb.Repository{
		StorageName:  "default",
		RelativePath: flag.Args()[0],
	}
	repo := localrepo.New(locator, cmdFactory, catfileCache, repoProto)
	headOID, err := repo.ResolveRevision(ctx, "HEAD")
	repoFiler := &repository.GitFiler{Ctx: ctx, Repo: repo, TreeishID: headOID}
	detectedLicenses, err := licensedb.Detect(repoFiler)
	if err != nil {
		panic(err)
	}

	fmt.Println(detectedLicenses)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
