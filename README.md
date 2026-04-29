# Modular Batch Command Manager

Aplicação Flask para executar, acompanhar e interromper batches modulares compostos por plugins configuráveis.

O projeto fornece uma interface web simples para disparar comandos sequenciais por módulo, acompanhar logs em tempo real, evitar execuções concorrentes do mesmo módulo e interromper processos em execução quando necessário.

## Propósito

Este sistema foi criado para centralizar a execução de rotinas batch locais de forma organizada e extensível.

Cada módulo define sua própria sequência de plugins em YAML. A aplicação carrega essa configuração, executa os plugins na ordem definida e interrompe o batch caso algum plugin falhe.

Casos de uso típicos:

- orquestrar scripts operacionais;
- executar rotinas analíticas em sequência;
- acompanhar logs de comandos longos via navegador;
- evitar execuções simultâneas acidentais;
- interromper processos manualmente quando necessário.

## Funcionalidades

- Módulos de usuário acessíveis por rota, como `/<id_do_modulo>`.
- Configuração de plugins por YAML.
- Criação e edição de módulos pela interface web.
- Validação de YAML antes de salvar.
- Variáveis por módulo com placeholders em comandos.
- Suporte a variáveis de ambiente e valores sensíveis mascarados.
- Execução sequencial de plugins.
- Plugin `command_line` para executar comandos shell.
- Captura de stdout/stderr em tempo real.
- Validação por exit code, string de erro e string de sucesso.
- Lock por módulo usando `filelock`.
- Logs persistidos da última execução em arquivos temporários do projeto.
- Limpeza dos logs/metadados temporários de módulos ao iniciar a aplicação.
- Recuperação do console ao sair e voltar para a página durante uma execução.
- Status por etapa na página do módulo, com recuperação ao recarregar a tela.
- Atualização de status/logs por SSE global, sem polling periódico do navegador.
- Botão `Clear` para limpar logs quando o módulo está parado.
- Botão `Kill` para interromper o plugin ativo.
- UI simples com Bootstrap via CDN.

## Stack

- Python
- Flask
- PyYAML
- filelock
- subprocess
- Server-Sent Events (SSE)
- Bootstrap
- pytest

## Estrutura

```text
.
├── app.py
├── modules/
│   ├── user/
│   │   └── *.yaml
│   └── system/
│       └── *.yaml
├── plugins/
│   ├── base.py
│   ├── command_line.py
│   └── variables.py
├── templates/
│   ├── index.html
│   ├── module_edit.html
│   └── module.html
├── locks/
├── temp/
├── tests/
├── requirements.txt
├── AGENT.md
└── README.md
```

## Configuração de Módulos

Cada módulo de usuário é definido em `modules/user/<nome>.yaml`. O nome do arquivo, sem `.yaml`, é o ID do módulo e deve conter apenas letras, números, `_` ou `-`.

Exemplo:

```yaml
name: Analítico
description: Executa as etapas analíticas do batch.
plugins:
  - id: preparar_analitico
    type: command_line
    description: Prepara o ambiente analítico.
    command: "echo Preparando modulo analitico"
    error_contains: "ERROR"
    success_contains: "analitico"

  - id: processar_analitico_com_sleep
    type: command_line
    description: Processa o batch analítico e valida a conclusão.
    command: "sleep 30 && echo Batch analitico concluido"
    error_contains: "ERROR"
    success_contains: "concluido"
```

Use `description` no módulo para explicar o objetivo geral e em cada plugin para explicar o que a etapa faz.

### Variáveis de Módulo

Um módulo pode declarar `variables:` para reutilizar valores nos comandos. O comando usa placeholders no formato `{nome_da_variavel}`.

Exemplo:

```yaml
name: Analítico
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
  - id: consultar_clickhouse
    type: command_line
    command: "clickhouse-client --database {database} --password {clickhouse_password} --query 'SELECT * FROM eventos LIMIT {batch_limit}'"
    error_contains: "ERROR"
```

Tipos suportados:

- `string`: valor textual.
- `integer`: valor inteiro, aceitando inteiro no YAML ou texto numérico.
- `sensitive`: valor textual que não deve aparecer em logs, metadados ou console.

Quando `value` estiver no formato `$NOME_ENV`, a aplicação resolve o valor a partir da variável de ambiente `NOME_ENV` no momento da execução. Se a variável de ambiente não existir, o plugin falha antes de iniciar o comando.

Regras importantes:

- Variáveis são declaradas no escopo do módulo e podem ser usadas por qualquer plugin `command_line` do módulo.
- Nomes de variáveis devem começar com letra ou `_` e conter apenas letras, números e `_`.
- Placeholders sem variável configurada fazem a execução falhar antes do comando iniciar.
- Em comandos string, valores substituídos são escapados com `shlex.quote` antes de executar com `shell=True`.
- Em comandos lista, cada item é executado como argumento direto, sem shell.
- Valores `sensitive` são substituídos por `****` nos logs e metadados. O mascaramento é literal: se um processo externo transformar o segredo, por exemplo usando hash ou encoding, essa transformação não é inferida.
- Para usar chaves literais em um comando quando `variables:` estiver configurado, escape como `{{` e `}}`.

## Executando Localmente

Crie e ative um ambiente virtual:

```bash
python3 -m venv venv
source venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

Inicie a aplicação:

```bash
flask --app app run
```

Acesse:

```text
http://127.0.0.1:5000
```

## Testes

```bash
pytest
```

## Extensibilidade

Novos plugins devem herdar de `BasePlugin` e implementar:

- `run()`: executa o plugin e emite eventos de log.
- `kill()`: interrompe a execução ativa do plugin.

O contrato completo e as regras de arquitetura estão documentados em `AGENT.md`.

## Edição de Módulos

A página principal possui o botão `Adicionar` e cada linha possui `Editar`.

A tela de edição trabalha com YAML bruto e oferece:

- `Validate`: valida sintaxe YAML, campos obrigatórios, tipos de plugin, variáveis e placeholders sem persistir.
- `Salvar`: valida novamente e grava em `modules/user/<nome>.yaml`.
- `Excluir`: remove o YAML do módulo e arquivos temporários relacionados.

O salvamento e a exclusão de um módulo em execução são bloqueados para evitar mudança de configuração durante o batch. A validação continua disponível.

## Status das Etapas

Na página do módulo, cada plugin aparece com um status individual:

- `Não iniciado`: antes da execução ou após limpar os logs.
- `Enfileirado`: etapa futura dentro do batch atual.
- `Executando`: etapa ativa.
- `Concluído`: etapa finalizada com sucesso.
- `Falhou`: etapa que interrompeu o batch por erro.
- `Interrompido`: etapa ativa quando o usuário solicitou `Kill`.

Esses estados são reconstruídos pelos eventos persistidos da execução, então continuam aparecendo corretamente ao recarregar a página durante ou após um batch.

## Observações de Segurança

O plugin `command_line` executa comandos no host. Portanto, os arquivos YAML devem ser tratados como configuração confiável.

Use `sensitive` para senhas, tokens e segredos. Não coloque segredos diretamente em `string` ou `integer`, pois esses valores podem aparecer nos logs.

Não exponha esta aplicação publicamente sem autenticação, autorização e revisão de segurança adequadas.

## Licença

Este projeto ainda não define uma licença. Antes de publicar no GitHub como open source, adicione um arquivo `LICENSE`.
