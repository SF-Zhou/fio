/*
 * chunk engine
 *
 * g++ -O2 -g -shared -rdynamic -fPIC -std=c++20 -o engines/chunk_engine \
 *   engines/chunk_engine.cc target/release/libchunk_engine.a -Itarget/cxxbridge
 *
 * to test it execute:
 *
 * LD_LIBRARY_PATH=./engines ./fio examples/chunk_engine.fio
 *
 */
#include <algorithm>
#include <assert.h>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdlib.h>

#include "../target/cxxbridge/chunk_engine/src/cxx.rs.h"

#include "../config-host.h"
#include "../fio.h"
#include "../optgroup.h"

std::ofstream logger{"/tmp/log.txt", std::ios::out};
hf3fs::chunk_engine::Engine *engine;
thread_local std::string filename;

struct chunk_engine_options {
  int dummy;
  char *engine_path;
};

static struct fio_option options[] = {
    {
        .name = "engine_path",
        .lname = "chunk engine path",
        .type = FIO_OPT_STR_STORE,
        .off1 = offsetof(struct chunk_engine_options, engine_path),
        .help = "chunk engine path",
        .def = "engine",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {
        .name = NULL,
    },
};

struct Wrapper {
  Wrapper(struct thread_data *td) {
    td->io_ops->flags |= FIO_SYNCIO;
    td_set_ioengine_flags(td);
  }

  static Wrapper *get(struct thread_data *td) {
    return reinterpret_cast<Wrapper *>(td->io_ops_data);
  }

  static int fio_io_end(struct thread_data *td, struct io_u *io_u, int ret) {
    if (io_u->file && ret >= 0 && ddir_rw(io_u->ddir)) {
      io_u->file->engine_pos = io_u->offset + ret;
    }

    if (ret != (int)io_u->xfer_buflen) {
      if (ret >= 0) {
        io_u->resid = io_u->xfer_buflen - ret;
        io_u->error = 0;
        return FIO_Q_COMPLETED;
      } else
        io_u->error = errno;
    }

    if (io_u->error) {
      // io_u_log_error(td, io_u);
      td_verror(td, io_u->error, "xfer");
    }

    return FIO_Q_COMPLETED;
  }

  static int update_chunks(struct thread_data *td, struct io_u *io_u) {
    constexpr size_t block_size = 16 * 1024 * 1024;
    auto remain = io_u->xfer_buflen;
    auto offset = io_u->offset;
    auto ret = 0;

    ::hf3fs::chunk_engine::UpdateReq req;
    req.without_checksum = true;
    req.is_syncing = true;
    req.update_ver = 1;
    req.chain_ver = 1;
    req.data = (uint64_t)io_u->xfer_buf;

    std::string key;
    key.reserve(filename.length() + sizeof(uint32_t));
    key = filename;
    key.resize(key.capacity(), '\0');
    rust::Slice<const uint8_t> chunk_id{(const uint8_t *)key.data(),
                                        key.length()};

    while (remain > 0) {
      uint32_t idx = offset / block_size;
      auto chunk_end = (idx + 1) * block_size;
      req.length = std::min<size_t>(remain, chunk_end - offset);
      req.offset = offset % block_size;
      *(uint32_t *)(&key[filename.length()]) = idx;

      std::string error;
      auto chunk = engine->update_raw_chunk(chunk_id, req, error);
      if (!error.empty()) {
        static std::mutex mutex;
        auto lock = std::unique_lock<std::mutex>(mutex);
        logger << "update error: " << error << std::endl;
        logger.flush();
        return -1;
      }

      auto old_pos = req.out_pos;
      engine->commit_raw_chunk(chunk_id, chunk, old_pos, false, error);
      if (!error.empty()) {
        static std::mutex mutex;
        auto lock = std::unique_lock<std::mutex>(mutex);
        logger << "commit error: " << error << std::endl;
        logger.flush();
        return -2;
      }

      ret += req.length;
      req.data += req.length;
      remain -= req.length;
      offset += req.length;
    }

    return ret;
  }

  fio_q_status fio_chunk_engine_queue(struct thread_data *td,
                                      struct io_u *io_u) {
    fio_ro_check(td, io_u);
    int ret = update_chunks(td, io_u);
    return (fio_q_status)fio_io_end(td, io_u, ret);
  }

  int fio_chunk_engine_open(struct thread_data *, struct fio_file *f) {
    filename = f->file_name;
    return 0;
  }
};

extern "C" {

static fio_q_status fio_chunk_engine_queue(struct thread_data *td,
                                           struct io_u *io_u) {
  return Wrapper::get(td)->fio_chunk_engine_queue(td, io_u);
}

static int fio_chunk_engine_open(struct thread_data *td, struct fio_file *f) {
  return Wrapper::get(td)->fio_chunk_engine_open(td, f);
}

static int fio_chunk_engine_init(struct thread_data *td) {
  auto &options = *reinterpret_cast<const chunk_engine_options *>(td->eo);

  static std::once_flag once;
  static bool error = 0;
  std::call_once(once, [&] {
    ::rust::Str engine_path{options.engine_path};
    std::string error;
    engine = hf3fs::chunk_engine::create(engine_path, true, error).into_raw();
    if (engine == nullptr) {
      logger << "create error: " << error << "\n";
      error = true;
    }
  });
  if (error) {
    return 1;
  }

  td->io_ops_data = new Wrapper(td);
  return 0;
}

static void fio_chunk_engine_cleanup(struct thread_data *td) {
  delete Wrapper::get(td);
}

static struct ioengine_ops ioengine;
void get_ioengine(struct ioengine_ops **ioengine_ptr) {
  *ioengine_ptr = &ioengine;

  ioengine.name = "chunk_engine";
  ioengine.version = FIO_IOOPS_VERSION;
  ioengine.queue = fio_chunk_engine_queue;
  ioengine.init = fio_chunk_engine_init;
  ioengine.cleanup = fio_chunk_engine_cleanup;
  ioengine.open_file = fio_chunk_engine_open;
  ioengine.flags = FIO_DISKLESSIO | FIO_SYNCIO | FIO_NODISKUTIL;
  ioengine.option_struct_size = sizeof(chunk_engine_options);
  ioengine.options = options;
}
}
