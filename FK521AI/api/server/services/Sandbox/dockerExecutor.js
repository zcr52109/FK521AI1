const crypto = require('crypto');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const { spawn } = require('child_process');
const mime = require('mime-types');
const { logger } = require('@fk521ai/data-schemas');
const {
  ensureConversationSandbox,
  getTaskDir,
  sanitizeSegment,
  buildSandboxScope,
} = require('./paths');
const { getSandboxIsolationConfig, chownPathBestEffort } = require('./isolation');
const { SANDBOX_PATHS } = require('./runtimeContract');

const DEFAULT_TIMEOUT_MS = Number(process.env.FK521_SANDBOX_TIMEOUT_MS || 120000);
const DEFAULT_MAX_OUTPUT_BYTES = Number(process.env.FK521_SANDBOX_MAX_OUTPUT_BYTES || 262144);
const PYTHON_IMAGE = process.env.FK521_SANDBOX_PYTHON_IMAGE || 'python:3.11-slim';
const NODE_IMAGE = process.env.FK521_SANDBOX_NODE_IMAGE || 'node:20-bookworm-slim';

function hashSuffix(value, length = 12) {
  return crypto
    .createHash('sha256')
    .update(String(value || ''))
    .digest('hex')
    .slice(0, length);
}

function buildSandboxIdentity({ conversationId, taskId, authContext = {} }) {
  const scope = buildSandboxScope(authContext);
  const conversationKey = sanitizeSegment(conversationId, 'new');
  const taskKey = sanitizeSegment(taskId, 'task');
  const identitySeed = `${scope.tenantSegment}:${scope.userSegment}:${conversationKey}:${taskKey}`;
  const scopeHash = hashSuffix(identitySeed, 16);
  const hostname = `sbx-${scope.tenantSegment}-${scope.userSegment}`.slice(0, 63);
  const containerName = `fk521ai-sbx-${scopeHash}`.slice(0, 63);
  const networkName = `fk521ai-sbx-net-${scopeHash}`.slice(0, 63);
  const isolation = getSandboxIsolationConfig(scope);

  return {
    ...scope,
    conversationKey,
    taskKey,
    scopeHash,
    hostname,
    containerName,
    networkName,
    labels: {
      'fk521.sandbox.managed': 'true',
      'fk521.sandbox.tenant': scope.tenantSegment,
      'fk521.sandbox.user': scope.userSegment,
      'fk521.sandbox.conversation': conversationKey,
      'fk521.sandbox.task': taskKey,
      'fk521.sandbox.scope': scopeHash,
      'fk521.sandbox.runtime': isolation.runtimeClassHint,
      'fk521.sandbox.userns': isolation.usernsMode || 'daemon-default',
    },
    containerUid: isolation.ownership.uid,
    containerGid: isolation.ownership.gid,
    isolation,
    environment: {
      FK521_SANDBOX_PRINCIPAL_ID: scope.principalId,
      FK521_SANDBOX_TENANT_ID: scope.tenantSegment,
      FK521_SANDBOX_USER_ID: scope.userSegment,
      FK521_SANDBOX_CONVERSATION_ID: String(conversationId || 'new'),
      FK521_SANDBOX_TASK_ID: String(taskId || 'task'),
      FK521_SANDBOX_SCOPE_ID: scopeHash,
      FK521_SANDBOX_UID: String(isolation.ownership.uid),
      FK521_SANDBOX_GID: String(isolation.ownership.gid),
      FK521_SANDBOX_RUNTIME: isolation.runtimeClassHint,
    },
  };
}

async function runDockerCommand(args) {
  return await new Promise((resolve, reject) => {
    const child = spawn('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });
    child.on('error', (error) => reject(error));
    child.on('close', (code) => {
      if (code === 0) {
        resolve({ stdout, stderr, code });
        return;
      }
      const error = new Error(
        stderr.trim() || stdout.trim() || `docker ${args[0]} failed with code ${code}`,
      );
      error.code = code;
      reject(error);
    });
  });
}

async function ensureSandboxNetwork(identity, networkMode, isolation = identity.isolation || {}) {
  if (networkMode !== 'bridge') {
    return { mode: networkMode, cleanupNetwork: null };
  }

  const createArgs = [
    'network',
    'create',
    '--driver',
    'bridge',
    '--opt',
    'com.docker.network.bridge.enable_icc=false',
    '--label',
    'fk521.sandbox.managed=true',
    '--label',
    `fk521.sandbox.tenant=${identity.tenantSegment}`,
    '--label',
    `fk521.sandbox.user=${identity.userSegment}`,
    '--label',
    `fk521.sandbox.scope=${identity.scopeHash}`,
  ];

  if (isolation.internalBridge) {
    createArgs.push('--internal');
  }

  createArgs.push(identity.networkName);
  await runDockerCommand(createArgs);

  return {
    mode: identity.networkName,
    cleanupNetwork: identity.networkName,
  };
}

async function cleanupSandboxNetwork(networkName) {
  if (!networkName) {
    return;
  }

  try {
    await runDockerCommand(['network', 'rm', networkName]);
  } catch (error) {
    logger.warn?.(`[sandbox] failed to remove network ${networkName}: ${error.message}`);
  }
}

function normalizeLanguage(language) {
  const normalized = String(language || '')
    .trim()
    .toLowerCase();
  if (['python', 'python3', 'py'].includes(normalized)) {
    return 'python';
  }
  if (['javascript', 'js', 'node', 'nodejs'].includes(normalized)) {
    return 'javascript';
  }
  throw new Error(`不支持的代码语言：${language}`);
}

function getRuntimeConfig(language) {
  if (language === 'python') {
    return {
      image: PYTHON_IMAGE,
      filename: 'main.py',
      command: ['python3', 'main.py'],
    };
  }

  return {
    image: NODE_IMAGE,
    filename: 'main.mjs',
    command: ['node', '--no-warnings', 'main.mjs'],
  };
}

async function pathExists(targetPath) {
  try {
    return await fsp.lstat(targetPath);
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

async function uniqueTaskShortcutPath(taskDir, preferredName) {
  const parsed = path.parse(preferredName);
  let candidate = preferredName;
  let counter = 1;

  while (await pathExists(path.join(taskDir, candidate))) {
    candidate = `${parsed.name}-uploaded${counter === 1 ? '' : '-' + counter}${parsed.ext}`;
    counter += 1;
  }

  return {
    filename: candidate,
    hostPath: path.join(taskDir, candidate),
  };
}

async function createTaskUploadShortcuts({ uploadsDir, taskDir }) {
  let entries = [];
  try {
    entries = await fsp.readdir(uploadsDir, { withFileTypes: true });
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return [];
    }
    throw error;
  }

  const shortcuts = [];
  const uploadDirShortcut = path.join(taskDir, 'uploads');
  if (!(await pathExists(uploadDirShortcut))) {
    try {
      await fsp.symlink(SANDBOX_PATHS.uploads, uploadDirShortcut, 'dir');
      shortcuts.push({
        filename: 'uploads',
        sandboxPath: SANDBOX_PATHS.uploads,
        type: 'directory',
      });
    } catch (error) {
      logger.warn?.(`[sandbox] failed to expose uploads shortcut in task dir: ${error.message}`);
    }
  }

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }

    const sourceHostPath = path.join(uploadsDir, entry.name);
    const shortcut = await uniqueTaskShortcutPath(taskDir, entry.name);
    const sandboxTarget = `${SANDBOX_PATHS.uploads}/${entry.name}`;

    try {
      await fsp.symlink(sandboxTarget, shortcut.hostPath, 'file');
      shortcuts.push({ filename: shortcut.filename, sandboxPath: sandboxTarget, type: 'file' });
    } catch (error) {
      try {
        await fsp.copyFile(sourceHostPath, shortcut.hostPath);
        shortcuts.push({
          filename: shortcut.filename,
          sandboxPath: `${SANDBOX_PATHS.workspace}/tasks/${path.basename(taskDir)}/${shortcut.filename}`,
          type: 'file-copy',
        });
      } catch (copyError) {
        logger.warn?.(
          `[sandbox] failed to expose upload ${entry.name} in task dir: ${copyError.message || error.message}`,
        );
      }
    }
  }

  return shortcuts;
}

async function readPrimaryProjectRoot(workspaceDir) {
  try {
    const manifestPath = path.join(workspaceDir, 'manifests', 'project-archives.json');
    const raw = await fsp.readFile(manifestPath, 'utf8');
    const parsed = JSON.parse(raw);
    const primaryProjectRoot = String(parsed?.primaryProjectRoot || '').trim();
    if (!primaryProjectRoot.startsWith(`${SANDBOX_PATHS.projects}/`)) {
      return null;
    }
    return primaryProjectRoot;
  } catch (_error) {
    return null;
  }
}

async function createTaskProjectShortcut({ workspaceDir, taskDir }) {
  const primaryProjectRoot = await readPrimaryProjectRoot(workspaceDir);
  if (!primaryProjectRoot) {
    return null;
  }

  const shortcutPath = path.join(taskDir, 'project');
  if (await pathExists(shortcutPath)) {
    return primaryProjectRoot;
  }

  try {
    await fsp.symlink(primaryProjectRoot, shortcutPath, 'dir');
    return primaryProjectRoot;
  } catch (_error) {
    return null;
  }
}

async function listFilesRecursively(baseDir) {
  const entries = [];
  async function walk(currentDir) {
    let dirEntries = [];
    try {
      dirEntries = await fsp.readdir(currentDir, { withFileTypes: true });
    } catch (_error) {
      return;
    }

    for (const entry of dirEntries) {
      if (entry.name.startsWith('.')) {
        continue;
      }
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      const stat = await fsp.stat(fullPath);
      entries.push({
        fullPath,
        relativePath: path.relative(baseDir, fullPath).replace(/\\/g, '/'),
        mtimeMs: stat.mtimeMs,
        size: stat.size,
      });
    }
  }
  await walk(baseDir);
  return entries;
}

function filesMap(entries) {
  return new Map(entries.map((entry) => [entry.relativePath, entry]));
}

function collectGeneratedFiles({
  beforeTaskFiles,
  beforeOutputFiles,
  afterTaskFiles,
  afterOutputFiles,
  entryFilename,
  taskId,
}) {
  const beforeTaskMap = filesMap(beforeTaskFiles);
  const beforeOutputMap = filesMap(beforeOutputFiles);
  const results = [];

  for (const entry of afterTaskFiles) {
    if (entry.relativePath === entryFilename || entry.relativePath.endsWith('/' + entryFilename)) {
      continue;
    }
    const previous = beforeTaskMap.get(entry.relativePath);
    if (!previous || previous.mtimeMs !== entry.mtimeMs || previous.size !== entry.size) {
      results.push({
        ...entry,
        sandboxRelativePath: `workspace/tasks/${taskId}/${entry.relativePath}`,
      });
    }
  }

  for (const entry of afterOutputFiles) {
    const previous = beforeOutputMap.get(entry.relativePath);
    if (!previous || previous.mtimeMs !== entry.mtimeMs || previous.size !== entry.size) {
      results.push({ ...entry, sandboxRelativePath: `outputs/${entry.relativePath}` });
    }
  }

  const deduped = new Map();
  for (const entry of results) {
    deduped.set(entry.fullPath, entry);
  }
  return [...deduped.values()];
}

function buildAttachment({ conversationId, taskId, file }) {
  const encodedConversation = encodeURIComponent(String(conversationId));
  const encodedPath = encodeURIComponent(file.sandboxRelativePath);
  const downloadPath = `/api/files/sandbox/${encodedConversation}?path=${encodedPath}`;
  return {
    filename: path.basename(file.relativePath),
    filepath: downloadPath,
    downloadPath,
    type: mime.lookup(file.fullPath) || 'application/octet-stream',
    bytes: file.size,
    object: 'file',
    embedded: false,
    source: 'execute_code',
    context: 'execute_code',
    conversationId,
    taskId,
    sandboxPath: file.sandboxRelativePath.startsWith('outputs/')
      ? `/workspace/${file.sandboxRelativePath}`
      : `/workspace/workdir/${file.sandboxRelativePath.slice('workspace/'.length)}`,
  };
}

async function ensureDockerAvailable() {
  return await new Promise((resolve, reject) => {
    const child = spawn('docker', ['info'], { stdio: 'ignore' });
    child.on('error', (error) => reject(new Error(`Docker 不可用: ${error.message}`)));
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error('Docker 不可用，请先启动 Docker 并确保 API 进程可访问 docker CLI'));
    });
  });
}

async function executeDockerSandbox({
  conversationId,
  taskId,
  language,
  code,
  networkMode = 'none',
  extraDockerArgs = [],
  environment = {},
  authContext = {},
}) {
  await ensureDockerAvailable();

  const normalizedLanguage = normalizeLanguage(language);
  const runtime = getRuntimeConfig(normalizedLanguage);
  const timeoutMs = DEFAULT_TIMEOUT_MS;
  const maxOutputBytes = DEFAULT_MAX_OUTPUT_BYTES;
  const safeTaskId = sanitizeSegment(taskId, 'task');
  const identity = buildSandboxIdentity({ conversationId, taskId: safeTaskId, authContext });
  const resolvedNetwork = await ensureSandboxNetwork(identity, networkMode, identity.isolation);
  const paths = ensureConversationSandbox(conversationId, authContext);
  const taskDir = getTaskDir(conversationId, safeTaskId, authContext);
  const entryPath = path.join(taskDir, runtime.filename);
  await fsp.writeFile(entryPath, String(code ?? ''), 'utf8');
  chownPathBestEffort(entryPath, identity.isolation.ownership);
  await createTaskUploadShortcuts({ uploadsDir: paths.uploadsDir, taskDir });
  await createTaskProjectShortcut({ workspaceDir: paths.workspaceDir, taskDir });

  const beforeTaskFiles = await listFilesRecursively(taskDir);
  const beforeOutputFiles = await listFilesRecursively(paths.outputsDir);

  const containerName = identity.containerName;
  const containerTaskDir = `${SANDBOX_PATHS.workspace}/tasks/${safeTaskId}`;
  const manifestHostPath = path.join(paths.workspaceDir, 'manifests', '.sandbox-capabilities.json');

  const dockerArgs = [
    'run',
    '--rm',
    '--name',
    containerName,
    '--network',
    resolvedNetwork.mode,
    '--hostname',
    identity.hostname,
    '-m',
    process.env.FK521_SANDBOX_MEMORY_LIMIT || '512m',
    '--cpus',
    process.env.FK521_SANDBOX_CPU_LIMIT || '1',
    '--read-only',
    '--tmpfs',
    '/tmp:rw,noexec,nosuid,size=64m',
    '--pids-limit',
    String(identity.isolation.pidsLimit),
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges:true',
    '-v',
    `${paths.uploadsDir}:${SANDBOX_PATHS.uploads}:ro`,
    '-v',
    `${paths.workspaceDir}:${SANDBOX_PATHS.workspace}:rw`,
    '-v',
    `${paths.projectsDir}:${SANDBOX_PATHS.projects}:rw`,
    '-v',
    `${paths.outputsDir}:${SANDBOX_PATHS.outputs}:rw`,
    '-v',
    `${path.join(paths.workspaceDir, 'manifests')}:${SANDBOX_PATHS.manifests}:rw`,
  ];

  dockerArgs.push('--user', `${identity.containerUid}:${identity.containerGid}`);

  if (identity.isolation.usernsMode) {
    dockerArgs.push('--userns', identity.isolation.usernsMode);
  }

  if (identity.isolation.seccompProfile && identity.isolation.seccompProfile !== 'default') {
    dockerArgs.push('--security-opt', `seccomp=${identity.isolation.seccompProfile}`);
  }

  if (identity.isolation.runtime) {
    dockerArgs.push('--runtime', identity.isolation.runtime);
  }

  if (fs.existsSync(manifestHostPath)) {
    dockerArgs.push('-v', `${manifestHostPath}:${SANDBOX_PATHS.runtimeCapabilities}:ro`);
  }

  for (const [key, value] of Object.entries(identity.labels)) {
    dockerArgs.push('--label', `${key}=${value}`);
  }

  const effectiveEnvironment = {
    ...identity.environment,
    ...(environment || {}),
  };

  for (const [key, value] of Object.entries(effectiveEnvironment)) {
    if (value == null) {
      continue;
    }
    dockerArgs.push('-e', `${key}=${String(value)}`);
  }

  if (Array.isArray(extraDockerArgs) && extraDockerArgs.length > 0) {
    dockerArgs.push(...extraDockerArgs);
  }

  dockerArgs.push('-w', containerTaskDir, runtime.image, ...runtime.command);

  const startedAt = Date.now();

  return await new Promise((resolve, reject) => {
    let stdout = '';
    let stderr = '';
    let finished = false;
    let overflowed = false;
    let timedOut = false;

    const child = spawn('docker', dockerArgs, {
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
        FK521_SANDBOX_UPLOADS_DIR: SANDBOX_PATHS.uploads,
        FK521_SANDBOX_WORKSPACE_DIR: SANDBOX_PATHS.workspace,
        FK521_SANDBOX_OUTPUTS_DIR: SANDBOX_PATHS.outputs,
        FK521_SANDBOX_CAPABILITY_MANIFEST: SANDBOX_PATHS.capabilityManifest,
        RUNTIME_CAPABILITIES: SANDBOX_PATHS.runtimeCapabilities,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const appendChunk = (target, chunk) => {
      const next = target + chunk.toString('utf8');
      if (Buffer.byteLength(next, 'utf8') > maxOutputBytes) {
        overflowed = true;
        child.kill('SIGKILL');
        return Buffer.from(next, 'utf8').subarray(0, maxOutputBytes).toString('utf8');
      }
      return next;
    };

    const finalize = async (error, exitCode, signal) => {
      if (finished) {
        return;
      }
      finished = true;
      clearTimeout(timer);

      await cleanupSandboxNetwork(resolvedNetwork.cleanupNetwork);

      if (error) {
        reject(error);
        return;
      }

      try {
        const afterTaskFiles = await listFilesRecursively(taskDir);
        const afterOutputFiles = await listFilesRecursively(paths.outputsDir);
        const generatedFiles = collectGeneratedFiles({
          beforeTaskFiles,
          beforeOutputFiles,
          afterTaskFiles,
          afterOutputFiles,
          entryFilename: runtime.filename,
          taskId: safeTaskId,
        });

        const attachments = generatedFiles.map((file) =>
          buildAttachment({ conversationId, taskId: safeTaskId, file }),
        );

        resolve({
          language: normalizedLanguage,
          image: runtime.image,
          exitCode: typeof exitCode === 'number' ? exitCode : -1,
          signal: signal || null,
          durationMs: Date.now() - startedAt,
          stdout,
          stderr,
          cwd: containerTaskDir,
          timedOut,
          overflowed,
          taskId: safeTaskId,
          taskDir: containerTaskDir,
          outputsDir: SANDBOX_PATHS.outputs,
          attachments,
          isolation: {
            runtime: identity.isolation.runtimeClassHint,
            usernsMode: identity.isolation.usernsMode || 'daemon-default',
            internalBridge: identity.isolation.internalBridge,
            perUserUidGid: true,
          },
        });
      } catch (scanError) {
        reject(scanError);
      }
    };

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
    }, timeoutMs);

    child.stdout.on('data', (chunk) => {
      stdout = appendChunk(stdout, chunk);
    });

    child.stderr.on('data', (chunk) => {
      stderr = appendChunk(stderr, chunk);
    });

    child.on('error', (error) => {
      logger.error('[sandbox] Docker task spawn failed', error);
      finalize(new Error(`启动 Docker 沙箱失败: ${error.message}`));
    });

    child.on('close', (exitCode, signal) => {
      if (timedOut) {
        stderr = `${stderr}\n执行超时：超过 ${timeoutMs}ms`.trim();
      }
      if (overflowed) {
        stderr = `${stderr}\n输出超过限制：${maxOutputBytes} bytes`.trim();
      }
      finalize(null, exitCode, signal);
    });
  });
}

module.exports = {
  executeDockerSandbox,
  normalizeLanguage,
  collectGeneratedFiles,
  buildSandboxIdentity,
  ensureSandboxNetwork,
  createTaskUploadShortcuts,
  createTaskProjectShortcut,
};
