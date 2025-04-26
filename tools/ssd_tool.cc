#include "arbiter/ssd_manager.h"
#include "common/ssd.h"

#include <getopt.h>
#include <cstdio>
#include <optional>
#include <string>

using namespace flint;
using namespace flint::arbiter;
using flint::arbiter::internal::g_db_client;

void print_usage() {
  printf("Usage: ssd_tool [options] [arguments]\n");
  printf("Options:\n");
  printf("  -h, --help          Display this help message\n");
  printf("  -d, --device        ssd to format\n");
  printf("  -f, --format        Format ssd\n");
  printf("      --format-all    Format all ssd\n");
  printf("  -a, --show-all      Display all ssd information\n");
  printf("\n");
}

static struct option long_opts[] = {{"help", no_argument, NULL, 'h'},
                                    {"device", required_argument, NULL, 'd'},
                                    {"format", no_argument, NULL, 'f'},
                                    {"format-all", no_argument, NULL, 0},
                                    {"show-all", no_argument, NULL, 'a'},
                                    {0, 0, 0, 0}};

int main(int argc, char* argv[]) {
  int opt;
  int option_index = 0;
  std::string ssd_path;
  bool format = false;
  bool show_all = false;
  bool format_all = false;

  if (argc == 1) {
    print_usage();
    return 1;
  }

  while ((opt = getopt_long(argc, argv, "d:hfa", long_opts, &option_index)) !=
         -1) {
    switch (opt) {
      case 'd':
        ssd_path.assign(optarg);
        break;
      case 'f':
        format = true;
        break;
      case 'a':
        show_all = true;
        break;
      case 'h':
        print_usage();
        return 0;
      case 0:
        if (strcmp(long_opts[option_index].name, "format-all") == 0) {
          format_all = true;
        }
        break;
      default:
        print_usage();
        return 1;
    }
  }

  if (format && ssd_path.empty()) {
    print_usage();
    return 1;
  }

  if (format_all) {
    g_db_client = std::make_shared<DBClient>();
    SsdPoolManager ssd_pool_manager;
    int ret =
        SsdManager::Create(ssd_pool_manager, std::nullopt, std::nullopt, false);
    if (ret < 0) {
      LOG_ERROR("Failed to create ssd pool");
      return -1;
    }
    for (auto& p : ssd_pool_manager) {
      auto& sm = p.second;
      sm->ResetMetadata();
    }
    g_db_client->Close();
    return 0;
  }

  if (show_all) {
    SsdPool ssd_pool;
    int ret = SsdPool::Create(ssd_pool, std::nullopt, std::nullopt);
    if (ret < 0) {
      LOG_ERROR("Failed to create ssd pool");
      return -1;
    }
    for (const auto& p : ssd_pool.ssds) {
      const auto& ssd = p.second;
      printf("%s, %s\n", ssd->sys_path.c_str(), ssd->nqn.c_str());
    }
  }

  return 0;
}