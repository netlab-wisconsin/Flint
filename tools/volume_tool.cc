#include "client/flint.h"
#include "common/interface.h"

#include <getopt.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

using namespace flint;

void PrintUsage() {
  printf("Usage: volume_tool [options] [arguments]\n");
  printf("Options:\n");
  printf("  -h, --help        Display this help message\n");
  printf("  -c, --create      Create volume\n");
  printf("  -r, --remove      Remove volume\n");
  printf("  -f, --rep-factor  Replication factor\n");
  printf("  -s, --size        Size of the volume in MiB\n");
  printf("  -p, --policy      Extent allocation policy of the volume\n");
  printf("  -v, --show        List single volume\n");
  printf("      --show-all    List all volumes\n");
  printf("\n");
}

static struct option long_opts[] = {
    {"help", no_argument, NULL, 'h'},
    {"create", required_argument, NULL, 'c'},
    {"remove", required_argument, NULL, 'r'},
    {"size", required_argument, NULL, 's'},
    {"policy", required_argument, NULL, 'p'},
    {"rep-factor", required_argument, NULL, 'f'},
    {"show", required_argument, NULL, 'v'},
    {"show-all", no_argument, NULL, 0},
    {0, 0, 0, 0},
};

static int ShowVolume(std::string vol_name) {
  env::FlintConfig options;
  env::Init(options);

  VolumeOptions volume_options;
  if (Volume::List(vol_name, volume_options) != 0) {
    LOG_ERROR("failed to list volume \"%s\"\n", vol_name.c_str());
    return -1;
  }
  printf("volume \"%s\" information:\n%s", vol_name.c_str(),
         volume_options.ToString().c_str());
  return 0;
}

static int ShowVolumes() {
  std::vector<VolumeOptions> all_options;
  if (Volume::ListAll(all_options) != 0) {
    LOG_ERROR("failed to list all volumes\n");
    return -1;
  }

  if (all_options.empty()) {
    printf("No volumes created.\n");
  } else {
    for (auto& options : all_options) {
      printf("volume \"%s\" information:\n%s\n", options.name.c_str(),
             options.ToString().c_str());
    }
  }
  return 0;
}

int main(int argc, char* argv[]) {
  int opt;
  bool create = false;
  bool remove = false;
  std::string vol_name;
  bool show_all = false;
  bool show_volume = false;
  uint64_t size = 0;
  VolumeOptions options;
  const char* opt_name;
  int long_opt_ind;
  int rf = 1;
  int ret = 0;
  ExtAllocPolicy policy = ExtAllocPolicy::kLazy;
  std::string policy_str;

  if (argc == 1) {
    PrintUsage();
    return 1;
  }

  while ((opt = getopt_long(argc, argv, "hc:r:s:f:v:p:", long_opts,
                            &long_opt_ind)) != -1) {
    switch (opt) {
      case 'c':
        create = true;
        vol_name = optarg;
        break;
      case 'r':
        remove = true;
        vol_name = optarg;
        break;
      case 's':
        size = MiB(std::stoull(optarg));
        break;
      case 'f':
        rf = std::stoi(optarg);
        if (rf < 1) {
          LOG_FATAL("replication factor must >= 1\n");
          PrintUsage();
          return 1;
        }
        break;
      case 'p':
        policy_str = optarg;
        if (policy_str == "lazy") {
          policy = ExtAllocPolicy::kLazy;
        } else if (policy_str == "full") {
          policy = ExtAllocPolicy::kFull;
        } else {
          LOG_FATAL("invalid extent allocation policy: %s\n", policy_str.c_str());
        }
        break;
      case 'v':
        show_volume = true;
        vol_name = optarg;
        break;
      case 0:
        opt_name = long_opts[long_opt_ind].name;
        if (!strcmp(opt_name, "show-all")) {
          show_all = true;
          break;
        }
      case 'h':
      case '?':
      default:
        PrintUsage();
        return 1;
        break;
    }
  }

  if (create && remove) {
    printf("cannot specify create and remove at the same time!\n");
    PrintUsage();
    return 1;
  }

  if (show_volume && (create || remove)) {
    printf("cannot specify show and create or remove at the same time!\n");
    PrintUsage();
    return 1;
  }

  if (show_volume && show_all) {
    printf("cannot specify show and show-all at the same time!\n");
    PrintUsage();
    return 1;
  }

  env::FlintConfig flint_options;
  env::Init(flint_options);

  if (show_volume) {
    ret = ShowVolume(vol_name);
    goto out;
  }

  if (show_all) {
    ret = ShowVolumes();
    goto out;
  }

  if (create) {
    if (size == 0) {
      printf("must specify volume size\n");
      PrintUsage();
      goto out;
    }
    if (rf == 1)
      printf(
          "WARNING: replication factor = 1. This should be used for testing "
          "only.\n");
    printf("creating volume \"%s\"...\n", vol_name.data());
    options.name = vol_name;
    options.size = size;
    options.rep_factor = rf;
    options.policy = static_cast<uint8_t>(policy);
    ret = Volume::Create(options);
    if (ret != 0) {
      LOG_ERROR("failed to create volume \"%s\"\n", vol_name.c_str());
      goto out;
    }
    printf("volume \"%s\" created successfully!\n", vol_name.data());
  }

  if (remove) {
    printf("removing volume \"%s\"\n", vol_name.c_str());
    ret = Volume::Delete(vol_name);
    if (ret != 0) {
      LOG_ERROR("failed to remove volume \"%s\"\n", vol_name.c_str());
      goto out;
    }
    printf("volume \"%s\" removed successfully!\n", vol_name.data());
  }

out:
  env::Close();
  return ret;
}