# AGENT.md

## Propósito do Projeto

Este projeto se chama Latch e implementa, em Python/Flask, um gerenciador de comandos batch modular para executar, acompanhar, interromper e limpar execuções sequenciais de plugins configurados por módulo.

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
- `modules/user/`: configurações YAML dos módulos criados pelo usuário.
- `modules/system/`: módulos internos usados por desenvolvimento e testes automatizados.
- `plugins/`: implementação dos tipos de plugin.
- `plugins/variables.py`: validação, resolução, substituição e mascaramento de variáveis de módulo usadas por comandos.
- `templates/`: UI HTML com Bootstrap CDN e JavaScript simples.
- `templates/_app_header.html` e `templates/_app_footer.html`: marca discreta e rodapé oficial compartilhados pelas páginas.
- `settings.yaml`: configuração local de autenticação com hashes dos usuários fixos `admin` e `user`.
- `locks/`: arquivos de lock por módulo usando `filelock`.
- `temp/`: logs e metadados temporários da execução, ignorados pelo Git.
- `tests/`: testes de contrato e comportamento.

Módulos de usuário:

- Todo arquivo válido em `modules/user/<module>.yaml` é descoberto automaticamente.
- O ID do módulo é o nome do arquivo sem `.yaml` e deve conter apenas letras, números, `_` ou `-`.
- Cada módulo é exposto em `/<module>`.

Configuração de módulo:

- Cada módulo de usuário possui um YAML em `modules/user/<module>.yaml`.
- O YAML define `name`, opcionalmente `description`, opcionalmente `variables`, e a lista ordenada `plugins`.
- Cada plugin pode declarar `description` textual para explicar a etapa.
- A ordem da lista é a ordem exata de execução.

Exemplo:

```yaml
name: Analítico
description: Executa consulta analítica no ClickHouse.
variables:
  database:
    type: string
    value: analytics
  batch_limit:
    type: integer
    value: 1000
  clickhouse_password:
    type: sensitive
    value: $CLICKHOUSE_PASSWORD
plugins:
  - id: processar_analitico
    type: command_line
    description: Consulta eventos analíticos respeitando o limite configurado.
    command: "clickhouse-client --database {database} --password {clickhouse_password} --query 'SELECT * FROM eventos LIMIT {batch_limit}'"
    error_contains: "ERROR"
```

Variáveis de módulo:

- `variables` é opcional e tem escopo por módulo.
- Cada variável deve usar o formato explícito `{type, value}`.
- Tipos suportados: `string`, `integer` e `sensitive`.
- `string` e `sensitive` exigem valor textual; `integer` aceita inteiro YAML ou texto numérico.
- Valores no formato `$NOME_ENV` são resolvidos a partir do ambiente no momento da criação do plugin.
- Variável de ambiente ausente falha antes de iniciar o comando.
- Nomes de variáveis e placeholders devem seguir `^[A-Za-z_][A-Za-z0-9_]*$`.
- Placeholders desconhecidos falham antes de iniciar o comando.
- Valores `sensitive` nunca devem aparecer em logs, console, SSE, JSON persistido ou metadados ativos; devem ser mascarados como `****`.
- O mascaramento é literal sobre o valor sensível resolvido. Transformações feitas por processos externos, como hash ou encoding, não são inferidas.

## Regras de Negócio

- O batch é executado sequencialmente, um plugin por vez.
- Se um plugin falhar, os próximos plugins não são executados.
- O lock é por módulo, não global.
- Um módulo em execução bloqueia nova execução simultânea do mesmo módulo.
- Módulos diferentes podem executar em paralelo.
- A página principal lista módulos em formato tabular, com status em coluna própria.
- O setup inicial cria senhas para os usuários fixos `admin` e `user`.
- `admin` pode criar, editar, validar e excluir módulos.
- `user` pode abrir módulos, executar batches, acompanhar status/logs, limpar logs e solicitar `Kill`, mas não pode editar módulos.
- A página principal permite adicionar módulos e editar módulos existentes apenas para `admin`.
- A página do módulo mostra plugins configurados em ordem, status por etapa, console, status geral e ações.
- A tela de edição usa YAML bruto e oferece `Validate`, `Save` e exclusão.
- `Validate` verifica sintaxe YAML, schema, tipo do plugin, variáveis, placeholders e instanciação do plugin sem executar comandos.
- `Save` persiste em `modules/user/<module>.yaml` e é bloqueado enquanto o módulo está em execução.
- A exclusão remove `modules/user/<module>.yaml`, `temp/temp_<module>.jsonl`, `temp/active_<module>.json` e `locks/<module>.lock`, e é bloqueada enquanto o módulo está em execução.
- Status por etapa:
  - `Não iniciado`: estado inicial da tela, antes de uma execução ou depois de limpar logs.
  - `Enfileirado`: etapa futura dentro da execução atual.
  - `Executando`: etapa ativa.
  - `Concluído`: etapa finalizada com sucesso.
  - `Falhou`: etapa que encerrou o batch por erro.
  - `Interrompido`: etapa ativa quando o batch foi morto pelo usuário.
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
- Se o módulo tiver `variables`, placeholders `{variavel}` são substituídos antes do `subprocess`.
- Em comando string, valores substituídos são escapados com `shlex.quote`.
- Em comando lista, valores substituídos viram texto dentro do argumento correspondente, sem shell quoting.
- Logs de comando iniciado e metadados usam a versão mascarada do comando.
- Linhas de stdout/stderr são mascaradas antes de virar `PluginEvent`.
- Se `variables` estiver configurado, chaves literais em comandos devem ser escapadas como `{{` e `}}`.
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
- `GET /modules/new`: tela para criar módulo, restrita ao `admin`.
- `GET /modules/<module>/edit`: tela para editar módulo, restrita ao `admin`.
- `GET /<module>`: página do módulo.
- `GET /api/modules/status`: status de todos os módulos.
- `GET /api/modules/<module>/status`: status de um módulo.
- `POST /api/modules/validate`: valida YAML de módulo sem persistir, restrita ao `admin`.
- `POST /api/modules`: cria módulo em `modules/user/<module>.yaml`, restrita ao `admin`.
- `PUT /api/modules/<module>`: salva YAML de módulo existente, restrita ao `admin`.
- `DELETE /api/modules/<module>`: exclui módulo e arquivos temporários relacionados, restrita ao `admin`.
- `GET /api/modules/<module>/run`: inicia execução via SSE.
- `GET /api/modules/<module>/logs`: lê logs persistidos incrementalmente.
- `POST /api/modules/<module>/logs/clear`: limpa logs quando parado.
- `POST /api/modules/<module>/kill`: solicita kill do plugin atual.
- `GET /api/events`: stream SSE global de sinais mínimos de atualização.

Persistência temporária:

- Logs da última execução ficam em `temp/temp_<module>.jsonl`.
- Cada linha é um evento JSON com `run_id`, `sequence`, `event`, `created_at`, `level`, `message`, status opcionais por etapa em `plugin_statuses` e metadados opcionais.
- Execução ativa fica em `temp/active_<module>.json`.
- A execução ativa inclui `plugin_statuses`, além de plugin atual, kill flag e metadados como PID/PGID.
- Arquivos em `temp/*.jsonl` e `temp/active_*.json` não devem ser versionados.
- Ao iniciar a aplicação, os artefatos gerados por módulos em `temp/temp_*.jsonl` e `temp/active_*.json` são removidos.

O `run_id` identifica uma execução. A `sequence` reinicia a cada nova execução. O frontend usa ambos para deduplicar eventos, detectar quando deve resetar o console e reconstruir status por etapa a partir dos eventos persistidos.

## Interface

Padrões atuais:

- Bootstrap via CDN.
- O estilo visual compartilhado fica em `static/app.css`; evite CSS inline nos templates salvo exceção pontual justificada.
- O visual padrão é inspirado no GitHub light: fundo claro, superfícies brancas, bordas finas, tabelas densas, azul para ação primária e vermelho apenas para ações destrutivas.
- HTML simples em Jinja templates.
- O nome público do produto é `Latch`.
- Páginas autenticadas devem mostrar o cabeçalho global com `Latch`, navegação para módulos, usuário logado e logout.
- Login e setup devem mostrar `Latch` com destaque visual moderado.
- Todas as páginas HTML devem incluir o rodapé compartilhado com a página oficial: `https://github.com/edgarrc/latch`.
- Logs renderizados com `textContent`, nunca `innerHTML`.
- SSE usado para execução iniciada pela própria página.
- SSE global usado para sinalizar alterações vindas do backend sem polling periódico.
- Endpoints de status/logs continuam sendo a fonte dos dados; o SSE global apenas invalida a tela e o frontend faz `fetch` quando recebe o sinal.

Módulos de sistema:

- Módulos em `modules/system/*.yaml` são reservados para testes automatizados e arquitetura interna.
- Eles não devem aparecer na listagem, status global ou rotas públicas da interface/API.
- Agentes podem editá-los livremente para cobrir comportamentos de teste.
- Módulos em `modules/user/*.yaml` são dados operacionais da instalação e não devem ser tratados como contrato fixo da suíte.

Estados da tela do módulo:

- `Pronto`: pode executar e limpar; kill desabilitado.
- `Executando`: run/clear desabilitados; kill habilitado.
- `Interrompendo`: kill já solicitado; kill desabilitado.
- `Concluído`: execução finalizou com sucesso.
- `Falhou`: execução finalizou com erro.
- `Interrompido`: execução foi morta pelo usuário.

Estados da lista de etapas:

- A tela inicial mostra todas as etapas como `Não iniciado`.
- Ao iniciar um batch, as etapas entram em `Enfileirado`; a etapa atual vira `Executando` em `plugin_start`.
- `plugin_done` marca a etapa como `Concluído`.
- `done` com status `failed` marca o plugin do payload como `Falhou`.
- `done` com status `killed` marca o plugin do payload como `Interrompido`.
- Etapas futuras permanecem `Enfileirado` se o batch parar antes de chegar nelas.

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
- Validação de `variables` no YAML.
- Substituição de placeholder em comando string e lista.
- Resolução de variável por ambiente.
- Falha por variável de ambiente ausente.
- Falha por placeholder desconhecido.
- Falha por `integer` inválido.
- Mascaramento de `sensitive` em logs, stdout/stderr persistidos, comando exibido e metadados ativos.
- Lock por módulo.
- Independência entre módulos.
- Persistência e leitura incremental de logs.
- Reset de console por novo `run_id`.
- Status por etapa em `plugin_statuses`, incluindo sucesso, falha e etapas futuras enfileiradas.
- Persistência de `plugin_statuses` em `temp/active_<module>.json` durante execução.
- Clear permitido quando parado.
- Clear bloqueado durante execução.
- Kill bloqueado quando parado.
- Kill chamando `plugin.kill()` quando ativo.
- `command_line` gravando PID/PGID e encerrando como `killed`.
- Descoberta dinâmica de módulos de usuário em `modules/user/*.yaml`.
- Validação de YAML de módulo sem persistir.
- Criação e edição de módulo persistindo em `modules/user/<module>.yaml`.
- Bloqueio de salvamento quando o módulo está em execução.
- Exclusão de módulo removendo YAML, log temporário, execução ativa temporária e lock.
- Bloqueio de exclusão quando o módulo está em execução.

Ao adicionar novos tipos de plugin, inclua testes específicos para `run()` e `kill()`.

## Dependências

Dependências atuais em `requirements.txt`:

- Flask
- PyYAML
- filelock
- pytest

Não introduza novas dependências sem necessidade clara. Prefira manter a aplicação simples e explícita.
