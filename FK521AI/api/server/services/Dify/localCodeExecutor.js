const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { logger } = require('@fk521ai/data-schemas');
const { readDifyConsoleConfig } = require('~/server/utils/difyConsoleConfig');

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

function resolveCommand(config, language) {
  if (language === 'python') {
    return {
      command: config.pythonCommand || 'python3',
      args: ['-I', 'main.py'],
      filename: 'main.py',
    };
  }

  return {
    command: config.nodeCommand || 'node',
    args: ['--no-warnings', 'main.mjs'],
    filename: 'main.mjs',
  };
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, options);
    let stdout = '';
    let stderr = '';

    child.stdout?.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });

    child.stderr?.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });

    child.on('error', (error) => {
      reject(error);
    });

    child.on('close', (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

async function ensurePythonEnvironment(config) {
  const autoSetupEnabled = process.env.FK521_LOCAL_PYTHON_AUTO_SETUP === 'true';
  if (!autoSetupEnabled) {
    return { command: config.pythonCommand || 'python3' };
  }

  const venvPath =
    process.env.FK521_LOCAL_PYTHON_VENV_PATH || path.resolve(os.tmpdir(), 'fk521ai-python-venv');
  const pythonBin = path.join(venvPath, 'bin', 'python');
  const pipBin = path.join(venvPath, 'bin', 'pip');

  if (!fs.existsSync(pythonBin)) {
    await runCommand('python3', ['-m', 'venv', venvPath]);
  }

  const packages = (process.env.FK521_LOCAL_PYTHON_PACKAGES || '')
    .split(/[,\s]+/)
    .map((pkg) => pkg.trim())
    .filter(Boolean);

  if (packages.length > 0) {
    const result = await runCommand(
      pipBin,
      ['install', '--disable-pip-version-check', ...packages],
      {
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    if (result.code !== 0) {
      throw new Error(`安装 Python 依赖失败: ${result.stderr || result.stdout}`);
    }
  }

  return { command: pythonBin };
}

async function verifyPythonFormat(pythonCommand, sessionDir, filename) {
  const shouldCheck = process.env.FK521_LOCAL_PYTHON_FORMAT_CHECK === 'true';
  if (!shouldCheck) {
    return;
  }

  const formatterModule = process.env.FK521_LOCAL_PYTHON_FORMATTER || 'black';
  const result = await runCommand(pythonCommand, ['-m', formatterModule, '--check', filename], {
    cwd: sessionDir,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  if (result.code !== 0) {
    throw new Error(
      `Python 代码格式检查未通过（${formatterModule} --check）。\n${(result.stderr || result.stdout).trim()}`,
    );
  }
}

async function executeLocalCode({ language, code }) {
  const currentConfig = readDifyConsoleConfig();
  const config = currentConfig.codeExecutor || {};
  if (config.enabled !== true) {
    throw new Error('本地代码执行器已被禁用');
  }

  const normalizedLanguage = normalizeLanguage(language);
  const { command: baseCommand, args, filename } = resolveCommand(config, normalizedLanguage);
  const baseWorkdir = config.workdir || path.resolve(os.tmpdir(), 'fk521ai-dify-executor');
  fs.mkdirSync(baseWorkdir, { recursive: true });
  const sessionDir = fs.mkdtempSync(path.join(baseWorkdir, 'session-'));
  const entryFile = path.join(sessionDir, filename);
  fs.writeFileSync(entryFile, String(code || ''), 'utf8');

  let command = baseCommand;
  if (normalizedLanguage === 'python') {
    try {
      const runtime = await ensurePythonEnvironment(config);
      command = runtime.command;
      await verifyPythonFormat(command, sessionDir, filename);
    } catch (error) {
      logger.warn('[localCodeExecutor] Python environment check failed', error);
      throw error;
    }
  }

  const startedAt = Date.now();
  const timeoutMs = Number(config.timeoutMs || 12000);
  const maxOutputBytes = Number(config.maxOutputBytes || 131072);

  return await new Promise((resolve, reject) => {
    let stdout = '';
    let stderr = '';
    let finished = false;
    let killedByOverflow = false;
    let killedByTimeout = false;

    const child = spawn(command, args, {
      cwd: sessionDir,
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
        NODE_ENV: process.env.NODE_ENV || 'production',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const finalize = (error, payload) => {
      if (finished) {
        return;
      }
      finished = true;
      clearTimeout(timer);
      if (error) {
        reject(error);
        return;
      }
      resolve(payload);
    };

    const appendChunk = (target, chunk) => {
      const next = target + chunk.toString('utf8');
      const limited = next.slice(0, maxOutputBytes);
      if (next.length > maxOutputBytes) {
        killedByOverflow = true;
        child.kill('SIGKILL');
      }
      return limited;
    };

    const timer = setTimeout(() => {
      killedByTimeout = true;
      child.kill('SIGKILL');
    }, timeoutMs);

    child.stdout.on('data', (chunk) => {
      stdout = appendChunk(stdout, chunk);
    });

    child.stderr.on('data', (chunk) => {
      stderr = appendChunk(stderr, chunk);
    });

    child.on('error', (error) => {
      finalize(new Error(`无法启动本地执行器命令 ${command}: ${error.message}`));
    });

    child.on('close', (exitCode, signal) => {
      const durationMs = Date.now() - startedAt;

      if (killedByTimeout) {
        finalize(null, {
          language: normalizedLanguage,
          exitCode: -1,
          signal: signal || 'SIGKILL',
          durationMs,
          stdout,
          stderr: `${stderr}\n执行超时：超过 ${timeoutMs}ms`.trim(),
          cwd: sessionDir,
          timedOut: true,
          overflowed: false,
        });
        return;
      }

      if (killedByOverflow) {
        finalize(null, {
          language: normalizedLanguage,
          exitCode: -1,
          signal: signal || 'SIGKILL',
          durationMs,
          stdout,
          stderr: `${stderr}\n输出超过限制：${maxOutputBytes} bytes`.trim(),
          cwd: sessionDir,
          timedOut: false,
          overflowed: true,
        });
        return;
      }

      finalize(null, {
        language: normalizedLanguage,
        exitCode: typeof exitCode === 'number' ? exitCode : -1,
        signal: signal || null,
        durationMs,
        stdout,
        stderr,
        cwd: sessionDir,
        timedOut: false,
        overflowed: false,
      });
    });
  });
}

module.exports = {
  executeLocalCode,
  normalizeLanguage,
};
