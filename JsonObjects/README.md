<H1>JsonObjects</H1>
Foobar is a Python library for dealing with word pluralization.

<H2>Installation</H2>
<p>Use the package manager pip to install foobar.</p>
<p><b>pip install JsonObjects</b></p>

<H2>Content</H2>
<p><b>jObject class</b> - bla bla bla</p>

<H2>Usage</H2>
<p></p><b>import JsonObjects</b></p>

<p>jAlunos = jo.jObject(json.loads(dados))</p>
<p>jLista = jo.jObject(jAlunos.value(['nome', 'turma.sala']))</p>
<p>print(jLista.getTable())</p>

<p>jAluno = jAlunos.item(0)</p>
<p>print(jAluno.get('cursos[0].Nome'))</p>

<H1>Contributing</H1>
<p>Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.</p>

<p>Please make sure to update tests as appropriate.</p>

<H1>License</H1>
<p><b>MIT</b></p>