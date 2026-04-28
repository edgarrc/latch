# AGENT.md

## Propósito do Projeto

Este projeto é uma aplicação Python/Flask que implementa um Gerenciador de Comandos Batch Modular. Ele permite executar, acompanhar, interromper e limpar execuções sequenciais de plugins configurados por módulo.

O objetivo é fornecer uma base reutilizável para orquestrar batches locais por módulos, com UI simples, logs em tempo real, persistência da última execução e controle seguro de concorrência por módulo.

Este documento deve ser usado por ferramentas de IA e agentes de desenvolvimento como fonte de contexto do projeto. Toda nova funcionalidade, regra de negócio, mudança arquitetural ou padrão relevante deve ser refletido aqui. O `AGENT.md` deve ser mantido atualizado junto com o código.

## Regras Operacionais do Repositório

- Antes de executar qualquer comando que interaja com arquivos fora da pasta atual do projeto, seja leitura ou escrita, peça autorização explícita ao usuário.
- Use `rg`/`rg --files` para busca e inspeção sempre que possível.
- Use `apply_patch` para criar ou editar arquivos manualmente.
- Não reverta alterações existentes sem pedido explícito do usuário.
- Não execute testes, Python, shell externo ou ferramentas que possam ler bibliotecas/arquivos fora do projeto sem autorização explícita.

## Arquitetura

Estrutura principal:

- `app.py`: entrypoint Flask, rotas HTTP, execução batch, locking, logs persistidos e controle de execução ativa.
- `modules/`: configurações YAML dos módulos.
- `plugins/`: implementação dos tipos de plugin.
- `templates/`: UI HTML com Bootstrap CDN e JavaScript simples.
- `locks/`: arquivos de lock por módulo usando `filelock`.
- `temp/`: logs e metadados temporários da execução, ignorados pelo Git.
- `tests/`: testes de contrato e comportamento.

Módulos suportados:

- `tri`, exposto em `/tri`.
- `analitico`, exposto em `/analitico`.

Configuração de módulo:

- Cada módulo possui um YAML em `modules/<module>.yaml`.
- O YAML define `name` e a lista ordenada `plugins`.
- A ordem da lista é a ordem exata de execução.

Exemplo:

```yaml
name: Analítico
plugins:
  - id: processar_analitico
    type: command_line
    command: "sleep 30 && echo Batch analitico concluido"
    error_contains: "ERROR"
    success_contains: "concluido"
```

## Regras de Negócio

- O batch é executado sequencialmente, um plugin por vez.
- Se um plugin falhar, os próximos plugins não são executados.
- O lock é por módulo, não global.
- Um módulo em execução bloqueia nova execução simultânea do mesmo módulo.
- Módulos diferentes podem executar em paralelo.
- A página principal lista módulos em formato tabular, com status em coluna própria.
- A página do módulo mostra plugins configurados, console, status e ações.
- O console deve mostrar logs em tempo real e também recuperar a saída da última execução persistida.
- Se o usuário sair da página e voltar durante uma execução, o console deve continuar mostrando a saída já gerada e seguir acompanhando novos logs.
- `Clear` apaga o log da última execução apenas se o módulo não estiver em execução.
- `Kill` interrompe o plugin atualmente em execução e encerra o batch como `killed`.

## Plugins

Todo plugin deve herdar de `BasePlugin`.

Contrato base:

- `run() -> Iterator[PluginEvent]`: executa o plugin e emite eventos de log.
- `kill() -> None`: interrompe a execução ativa do plugin.
- `set_runtime_context(...)`: recebe contexto de módulo/run e callback para atualizar metadados de execução.

Erros padronizados:

- `PluginExecutionError`: falha normal do plugin.
- `PluginKillError`: falha ao tentar interromper o plugin.
- `PluginKilledError`: plugin interrompido por solicitação do usuário.

Novos tipos de plugin devem:

- Implementar `run()`.
- Implementar `kill()`.
- Emitir logs claros.
- Registrar metadados úteis via `update_runtime_metadata()`, quando aplicável.
- Encerrar recursos/processos no `finally`.

## Plugin `command_line`

O tipo `command_line` executa comandos no shell do host usando `subprocess`.

Regras:

- `command` pode ser string ou lista de strings.
- String usa `shell=True`.
- Lista usa execução direta.
- O processo é iniciado com `start_new_session=True`, criando grupo próprio.
- O PID e PGID são gravados nos metadados ativos em `temp/active_<module>.json`.
- O kill é feito por grupo de processo via shell:

```sh
kill -KILL -<pgid>
```

- O cleanup defensivo tenta:

```sh
kill -TERM -<pgid>
```

e depois:

```sh
kill -KILL -<pgid>
```

Validações:

- Exit code diferente de `0` é erro.
- Se `error_contains` aparecer no output, é erro.
- Se `success_contains` estiver configurado e não aparecer no output, é erro.
- Output deve ser capturado em tempo real de stdout/stderr.

## Execução, Status e Logs

Rotas principais:

- `GET /`: página principal com tabela de módulos.
- `GET /tri`: página do módulo TRI.
- `GET /analitico`: página do módulo analítico.
- `GET /api/modules/status`: status de todos os módulos.
- `GET /api/modules/<module>/status`: status de um módulo.
- `GET /api/modules/<module>/run`: inicia execução via SSE.
- `GET /api/modules/<module>/logs`: lê logs persistidos incrementalmente.
- `POST /api/modules/<module>/logs/clear`: limpa logs quando parado.
- `POST /api/modules/<module>/kill`: solicita kill do plugin atual.

Persistência temporária:

- Logs da última execução ficam em `temp/temp_<module>.jsonl`.
- Cada linha é um evento JSON com `run_id`, `sequence`, `event`, `created_at`, `level`, `message` e metadados opcionais.
- Execução ativa fica em `temp/active_<module>.json`.
- Arquivos em `temp/*.jsonl` e `temp/active_*.json` não devem ser versionados.

O `run_id` identifica uma execução. A `sequence` reinicia a cada nova execução. O frontend usa ambos para deduplicar eventos e detectar quando deve resetar o console.

## Interface

Padrões atuais:

- Bootstrap via CDN.
- HTML simples em Jinja templates.
- Logs renderizados com `textContent`, nunca `innerHTML`.
- SSE usado para execução iniciada pela própria página.
- Polling de logs usado para recuperar execução em andamento e como fonte persistida.

Estados da tela do módulo:

- `Pronto`: pode executar e limpar; kill desabilitado.
- `Executando`: run/clear desabilitados; kill habilitado.
- `Interrompendo`: kill já solicitado; kill desabilitado.
- `Concluído`: execução finalizou com sucesso.
- `Falhou`: execução finalizou com erro.
- `Interrompido`: execução foi morta pelo usuário.

## Concorrência

- `filelock` é obrigatório para lock por módulo.
- O lock é gravado em `locks/<module>.lock`.
- O estado em memória (`ACTIVE_RUNS`) complementa o lock para acessar o plugin ativo e permitir kill.
- O arquivo `temp/active_<module>.json` é observabilidade/metadados, não a fonte principal para chamar `kill()`.
- Em todos os caminhos de saída, o app deve liberar lock, remover execução ativa e limpar metadados ativos.

## Testes Esperados

Manter cobertura para:

- Carregamento de módulos YAML.
- Execução sequencial com sucesso.
- Falha por exit code.
- Falha por `error_contains`.
- Falha por ausência de `success_contains`.
- Lock por módulo.
- Independência entre módulos.
- Persistência e leitura incremental de logs.
- Reset de console por novo `run_id`.
- Clear permitido quando parado.
- Clear bloqueado durante execução.
- Kill bloqueado quando parado.
- Kill chamando `plugin.kill()` quando ativo.
- `command_line` gravando PID/PGID e encerrando como `killed`.

Ao adicionar novos tipos de plugin, inclua testes específicos para `run()` e `kill()`.

## Dependências

Dependências atuais em `requirements.txt`:

- Flask
- PyYAML
- filelock
- pytest

Não introduza novas dependências sem necessidade clara. Prefira manter a aplicação simples e explícita.

