// Code generated by qtc from "web.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

//line app/vmalert/web.qtpl:1
package main

//line app/vmalert/web.qtpl:3
import (
	"path"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/notifier"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/tpl"
)

//line app/vmalert/web.qtpl:13
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vmalert/web.qtpl:13
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vmalert/web.qtpl:13
func StreamWelcome(qw422016 *qt422016.Writer) {
//line app/vmalert/web.qtpl:13
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:14
	tpl.StreamHeader(qw422016, "vmalert", navItems)
//line app/vmalert/web.qtpl:14
	qw422016.N().S(`
    <p>
        API:<br>
        `)
//line app/vmalert/web.qtpl:17
	for _, p := range apiLinks {
//line app/vmalert/web.qtpl:17
		qw422016.N().S(`
            `)
//line app/vmalert/web.qtpl:19
		p, doc := p[0], p[1]

//line app/vmalert/web.qtpl:20
		qw422016.N().S(`
        	<a href="`)
//line app/vmalert/web.qtpl:21
		qw422016.E().S(p)
//line app/vmalert/web.qtpl:21
		qw422016.N().S(`">`)
//line app/vmalert/web.qtpl:21
		qw422016.E().S(p)
//line app/vmalert/web.qtpl:21
		qw422016.N().S(`</a> - `)
//line app/vmalert/web.qtpl:21
		qw422016.E().S(doc)
//line app/vmalert/web.qtpl:21
		qw422016.N().S(`<br/>
        `)
//line app/vmalert/web.qtpl:22
	}
//line app/vmalert/web.qtpl:22
	qw422016.N().S(`
    </p>
    `)
//line app/vmalert/web.qtpl:24
	tpl.StreamFooter(qw422016)
//line app/vmalert/web.qtpl:24
	qw422016.N().S(`
`)
//line app/vmalert/web.qtpl:25
}

//line app/vmalert/web.qtpl:25
func WriteWelcome(qq422016 qtio422016.Writer) {
//line app/vmalert/web.qtpl:25
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/web.qtpl:25
	StreamWelcome(qw422016)
//line app/vmalert/web.qtpl:25
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/web.qtpl:25
}

//line app/vmalert/web.qtpl:25
func Welcome() string {
//line app/vmalert/web.qtpl:25
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/web.qtpl:25
	WriteWelcome(qb422016)
//line app/vmalert/web.qtpl:25
	qs422016 := string(qb422016.B)
//line app/vmalert/web.qtpl:25
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/web.qtpl:25
	return qs422016
//line app/vmalert/web.qtpl:25
}

//line app/vmalert/web.qtpl:27
func StreamListGroups(qw422016 *qt422016.Writer, groups []APIGroup) {
//line app/vmalert/web.qtpl:27
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:28
	tpl.StreamHeader(qw422016, "Groups", navItems)
//line app/vmalert/web.qtpl:28
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:29
	if len(groups) > 0 {
//line app/vmalert/web.qtpl:29
		qw422016.N().S(`
        `)
//line app/vmalert/web.qtpl:31
		rOk := make(map[string]int)
		rNotOk := make(map[string]int)
		for _, g := range groups {
			for _, r := range g.Rules {
				if r.LastError != "" {
					rNotOk[g.Name]++
				} else {
					rOk[g.Name]++
				}
			}
		}

//line app/vmalert/web.qtpl:42
		qw422016.N().S(`
         <a class="btn btn-primary" role="button" onclick="collapseAll()">Collapse All</a>
         <a class="btn btn-primary" role="button" onclick="expandAll()">Expand All</a>
        `)
//line app/vmalert/web.qtpl:45
		for _, g := range groups {
//line app/vmalert/web.qtpl:45
			qw422016.N().S(`
              <div class="group-heading`)
//line app/vmalert/web.qtpl:46
			if rNotOk[g.Name] > 0 {
//line app/vmalert/web.qtpl:46
				qw422016.N().S(` alert-danger`)
//line app/vmalert/web.qtpl:46
			}
//line app/vmalert/web.qtpl:46
			qw422016.N().S(`"  data-bs-target="rules-`)
//line app/vmalert/web.qtpl:46
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:46
			qw422016.N().S(`">
                <span class="anchor" id="group-`)
//line app/vmalert/web.qtpl:47
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:47
			qw422016.N().S(`"></span>
                <a href="#group-`)
//line app/vmalert/web.qtpl:48
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:48
			qw422016.N().S(`">`)
//line app/vmalert/web.qtpl:48
			qw422016.E().S(g.Name)
//line app/vmalert/web.qtpl:48
			if g.Type != "prometheus" {
//line app/vmalert/web.qtpl:48
				qw422016.N().S(` (`)
//line app/vmalert/web.qtpl:48
				qw422016.E().S(g.Type)
//line app/vmalert/web.qtpl:48
				qw422016.N().S(`)`)
//line app/vmalert/web.qtpl:48
			}
//line app/vmalert/web.qtpl:48
			qw422016.N().S(` (every `)
//line app/vmalert/web.qtpl:48
			qw422016.N().FPrec(g.Interval, 0)
//line app/vmalert/web.qtpl:48
			qw422016.N().S(`s)</a>
                 `)
//line app/vmalert/web.qtpl:49
			if rNotOk[g.Name] > 0 {
//line app/vmalert/web.qtpl:49
				qw422016.N().S(`<span class="badge bg-danger" title="Number of rules with status Error">`)
//line app/vmalert/web.qtpl:49
				qw422016.N().D(rNotOk[g.Name])
//line app/vmalert/web.qtpl:49
				qw422016.N().S(`</span> `)
//line app/vmalert/web.qtpl:49
			}
//line app/vmalert/web.qtpl:49
			qw422016.N().S(`
                <span class="badge bg-success" title="Number of rules withs status Ok">`)
//line app/vmalert/web.qtpl:50
			qw422016.N().D(rOk[g.Name])
//line app/vmalert/web.qtpl:50
			qw422016.N().S(`</span>
                <p class="fs-6 fw-lighter">`)
//line app/vmalert/web.qtpl:51
			qw422016.E().S(g.File)
//line app/vmalert/web.qtpl:51
			qw422016.N().S(`</p>
                `)
//line app/vmalert/web.qtpl:52
			if len(g.Params) > 0 {
//line app/vmalert/web.qtpl:52
				qw422016.N().S(`
                    <div class="fs-6 fw-lighter">Extra params
                    `)
//line app/vmalert/web.qtpl:54
				for _, param := range g.Params {
//line app/vmalert/web.qtpl:54
					qw422016.N().S(`
                            <span class="float-left badge bg-primary">`)
//line app/vmalert/web.qtpl:55
					qw422016.E().S(param)
//line app/vmalert/web.qtpl:55
					qw422016.N().S(`</span>
                    `)
//line app/vmalert/web.qtpl:56
				}
//line app/vmalert/web.qtpl:56
				qw422016.N().S(`
                    </div>
                `)
//line app/vmalert/web.qtpl:58
			}
//line app/vmalert/web.qtpl:58
			qw422016.N().S(`
            </div>
            <div class="collapse" id="rules-`)
//line app/vmalert/web.qtpl:60
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:60
			qw422016.N().S(`">
                <table class="table table-striped table-hover table-sm">
                    <thead>
                        <tr>
                            <th scope="col">Rule</th>
                            <th scope="col" title="Shows if rule's execution ended with error">Error</th>
                            <th scope="col" title="How many samples were produced by the rule">Samples</th>
                            <th scope="col" title="How many seconds ago rule was executed">Updated</th>
                        </tr>
                    </thead>
                    <tbody>
                    `)
//line app/vmalert/web.qtpl:71
			for _, r := range g.Rules {
//line app/vmalert/web.qtpl:71
				qw422016.N().S(`
                        <tr`)
//line app/vmalert/web.qtpl:72
				if r.LastError != "" {
//line app/vmalert/web.qtpl:72
					qw422016.N().S(` class="alert-danger"`)
//line app/vmalert/web.qtpl:72
				}
//line app/vmalert/web.qtpl:72
				qw422016.N().S(`>
                            <td>
                                `)
//line app/vmalert/web.qtpl:74
				if r.Type == "alerting" {
//line app/vmalert/web.qtpl:74
					qw422016.N().S(`
                                <b>alert:</b> (for: `)
//line app/vmalert/web.qtpl:75
					qw422016.E().V(r.Duration)
//line app/vmalert/web.qtpl:75
					qw422016.N().S(`)
                                `)
//line app/vmalert/web.qtpl:76
				} else {
//line app/vmalert/web.qtpl:76
					qw422016.N().S(`
                                <b>record:</b> `)
//line app/vmalert/web.qtpl:77
					qw422016.E().S(r.Name)
//line app/vmalert/web.qtpl:77
					qw422016.N().S(`
                                `)
//line app/vmalert/web.qtpl:78
				}
//line app/vmalert/web.qtpl:78
				qw422016.N().S(`
                                <br>
                                <code><pre class="text-wrap">`)
//line app/vmalert/web.qtpl:80
				qw422016.E().S(r.Query)
//line app/vmalert/web.qtpl:80
				qw422016.N().S(`</pre></code><br>
                                `)
//line app/vmalert/web.qtpl:81
				if len(r.Labels) > 0 {
//line app/vmalert/web.qtpl:81
					qw422016.N().S(` <b>Labels:</b>`)
//line app/vmalert/web.qtpl:81
				}
//line app/vmalert/web.qtpl:81
				qw422016.N().S(`
                                `)
//line app/vmalert/web.qtpl:82
				for k, v := range r.Labels {
//line app/vmalert/web.qtpl:82
					qw422016.N().S(`
                                        <span class="ms-1 badge bg-primary">`)
//line app/vmalert/web.qtpl:83
					qw422016.E().S(k)
//line app/vmalert/web.qtpl:83
					qw422016.N().S(`=`)
//line app/vmalert/web.qtpl:83
					qw422016.E().S(v)
//line app/vmalert/web.qtpl:83
					qw422016.N().S(`</span>
                                `)
//line app/vmalert/web.qtpl:84
				}
//line app/vmalert/web.qtpl:84
				qw422016.N().S(`
                            </td>
                            <td><div class="error-cell">`)
//line app/vmalert/web.qtpl:86
				qw422016.E().S(r.LastError)
//line app/vmalert/web.qtpl:86
				qw422016.N().S(`</div></td>
                            <td>`)
//line app/vmalert/web.qtpl:87
				qw422016.N().D(r.LastSamples)
//line app/vmalert/web.qtpl:87
				qw422016.N().S(`</td>
                            <td>`)
//line app/vmalert/web.qtpl:88
				qw422016.N().FPrec(time.Since(r.LastEvaluation).Seconds(), 3)
//line app/vmalert/web.qtpl:88
				qw422016.N().S(`s ago</td>
                        </tr>
                    `)
//line app/vmalert/web.qtpl:90
			}
//line app/vmalert/web.qtpl:90
			qw422016.N().S(`
                 </tbody>
                </table>
            </div>
        `)
//line app/vmalert/web.qtpl:94
		}
//line app/vmalert/web.qtpl:94
		qw422016.N().S(`

    `)
//line app/vmalert/web.qtpl:96
	} else {
//line app/vmalert/web.qtpl:96
		qw422016.N().S(`
        <div>
            <p>No items...</p>
        </div>
    `)
//line app/vmalert/web.qtpl:100
	}
//line app/vmalert/web.qtpl:100
	qw422016.N().S(`

    `)
//line app/vmalert/web.qtpl:102
	tpl.StreamFooter(qw422016)
//line app/vmalert/web.qtpl:102
	qw422016.N().S(`

`)
//line app/vmalert/web.qtpl:104
}

//line app/vmalert/web.qtpl:104
func WriteListGroups(qq422016 qtio422016.Writer, groups []APIGroup) {
//line app/vmalert/web.qtpl:104
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/web.qtpl:104
	StreamListGroups(qw422016, groups)
//line app/vmalert/web.qtpl:104
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/web.qtpl:104
}

//line app/vmalert/web.qtpl:104
func ListGroups(groups []APIGroup) string {
//line app/vmalert/web.qtpl:104
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/web.qtpl:104
	WriteListGroups(qb422016, groups)
//line app/vmalert/web.qtpl:104
	qs422016 := string(qb422016.B)
//line app/vmalert/web.qtpl:104
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/web.qtpl:104
	return qs422016
//line app/vmalert/web.qtpl:104
}

//line app/vmalert/web.qtpl:107
func StreamListAlerts(qw422016 *qt422016.Writer, pathPrefix string, groupAlerts []GroupAlerts) {
//line app/vmalert/web.qtpl:107
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:108
	tpl.StreamHeader(qw422016, "Alerts", navItems)
//line app/vmalert/web.qtpl:108
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:109
	if len(groupAlerts) > 0 {
//line app/vmalert/web.qtpl:109
		qw422016.N().S(`
         <a class="btn btn-primary" role="button" onclick="collapseAll()">Collapse All</a>
         <a class="btn btn-primary" role="button" onclick="expandAll()">Expand All</a>
         `)
//line app/vmalert/web.qtpl:112
		for _, ga := range groupAlerts {
//line app/vmalert/web.qtpl:112
			qw422016.N().S(`
            `)
//line app/vmalert/web.qtpl:113
			g := ga.Group

//line app/vmalert/web.qtpl:113
			qw422016.N().S(`
            <div class="group-heading alert-danger" data-bs-target="rules-`)
//line app/vmalert/web.qtpl:114
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:114
			qw422016.N().S(`">
                <span class="anchor" id="group-`)
//line app/vmalert/web.qtpl:115
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:115
			qw422016.N().S(`"></span>
                <a href="#group-`)
//line app/vmalert/web.qtpl:116
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:116
			qw422016.N().S(`">`)
//line app/vmalert/web.qtpl:116
			qw422016.E().S(g.Name)
//line app/vmalert/web.qtpl:116
			if g.Type != "prometheus" {
//line app/vmalert/web.qtpl:116
				qw422016.N().S(` (`)
//line app/vmalert/web.qtpl:116
				qw422016.E().S(g.Type)
//line app/vmalert/web.qtpl:116
				qw422016.N().S(`)`)
//line app/vmalert/web.qtpl:116
			}
//line app/vmalert/web.qtpl:116
			qw422016.N().S(`</a>
                <span class="badge bg-danger" title="Number of active alerts">`)
//line app/vmalert/web.qtpl:117
			qw422016.N().D(len(ga.Alerts))
//line app/vmalert/web.qtpl:117
			qw422016.N().S(`</span>
                <br>
                <p class="fs-6 fw-lighter">`)
//line app/vmalert/web.qtpl:119
			qw422016.E().S(g.File)
//line app/vmalert/web.qtpl:119
			qw422016.N().S(`</p>
            </div>
            `)
//line app/vmalert/web.qtpl:122
			var keys []string
			alertsByRule := make(map[string][]*APIAlert)
			for _, alert := range ga.Alerts {
				if len(alertsByRule[alert.RuleID]) < 1 {
					keys = append(keys, alert.RuleID)
				}
				alertsByRule[alert.RuleID] = append(alertsByRule[alert.RuleID], alert)
			}
			sort.Strings(keys)

//line app/vmalert/web.qtpl:131
			qw422016.N().S(`
            <div class="collapse" id="rules-`)
//line app/vmalert/web.qtpl:132
			qw422016.E().S(g.ID)
//line app/vmalert/web.qtpl:132
			qw422016.N().S(`">
                `)
//line app/vmalert/web.qtpl:133
			for _, ruleID := range keys {
//line app/vmalert/web.qtpl:133
				qw422016.N().S(`
                    `)
//line app/vmalert/web.qtpl:135
				defaultAR := alertsByRule[ruleID][0]
				var labelKeys []string
				for k := range defaultAR.Labels {
					labelKeys = append(labelKeys, k)
				}
				sort.Strings(labelKeys)

//line app/vmalert/web.qtpl:141
				qw422016.N().S(`
                    <br>
                    <b>alert:</b> `)
//line app/vmalert/web.qtpl:143
				qw422016.E().S(defaultAR.Name)
//line app/vmalert/web.qtpl:143
				qw422016.N().S(` (`)
//line app/vmalert/web.qtpl:143
				qw422016.N().D(len(alertsByRule[ruleID]))
//line app/vmalert/web.qtpl:143
				qw422016.N().S(`)
                     | <span><a target="_blank" href="`)
//line app/vmalert/web.qtpl:144
				qw422016.E().S(defaultAR.SourceLink)
//line app/vmalert/web.qtpl:144
				qw422016.N().S(`">Source</a></span>
                    <br>
                    <b>expr:</b><code><pre>`)
//line app/vmalert/web.qtpl:146
				qw422016.E().S(defaultAR.Expression)
//line app/vmalert/web.qtpl:146
				qw422016.N().S(`</pre></code>
                    <table class="table table-striped table-hover table-sm">
                        <thead>
                            <tr>
                                <th scope="col">Labels</th>
                                <th scope="col">State</th>
                                <th scope="col">Active at</th>
                                <th scope="col">Value</th>
                                <th scope="col">Link</th>
                            </tr>
                        </thead>
                        <tbody>
                        `)
//line app/vmalert/web.qtpl:158
				for _, ar := range alertsByRule[ruleID] {
//line app/vmalert/web.qtpl:158
					qw422016.N().S(`
                            <tr>
                                <td>
                                    `)
//line app/vmalert/web.qtpl:161
					for _, k := range labelKeys {
//line app/vmalert/web.qtpl:161
						qw422016.N().S(`
                                        <span class="ms-1 badge bg-primary">`)
//line app/vmalert/web.qtpl:162
						qw422016.E().S(k)
//line app/vmalert/web.qtpl:162
						qw422016.N().S(`=`)
//line app/vmalert/web.qtpl:162
						qw422016.E().S(ar.Labels[k])
//line app/vmalert/web.qtpl:162
						qw422016.N().S(`</span>
                                    `)
//line app/vmalert/web.qtpl:163
					}
//line app/vmalert/web.qtpl:163
					qw422016.N().S(`
                                </td>
                                <td>`)
//line app/vmalert/web.qtpl:165
					streambadgeState(qw422016, ar.State)
//line app/vmalert/web.qtpl:165
					qw422016.N().S(`</td>
                                <td>
                                    `)
//line app/vmalert/web.qtpl:167
					qw422016.E().S(ar.ActiveAt.Format("2006-01-02T15:04:05Z07:00"))
//line app/vmalert/web.qtpl:167
					qw422016.N().S(`
                                    `)
//line app/vmalert/web.qtpl:168
					if ar.Restored {
//line app/vmalert/web.qtpl:168
						streambadgeRestored(qw422016)
//line app/vmalert/web.qtpl:168
					}
//line app/vmalert/web.qtpl:168
					qw422016.N().S(`
                                </td>
                                <td>`)
//line app/vmalert/web.qtpl:170
					qw422016.E().S(ar.Value)
//line app/vmalert/web.qtpl:170
					qw422016.N().S(`</td>
                                <td>
                                    <a href="`)
//line app/vmalert/web.qtpl:172
					qw422016.E().S(path.Join(pathPrefix, g.ID, ar.ID, "status"))
//line app/vmalert/web.qtpl:172
					qw422016.N().S(`">Details</a>
                                </td>
                            </tr>
                        `)
//line app/vmalert/web.qtpl:175
				}
//line app/vmalert/web.qtpl:175
				qw422016.N().S(`
                     </tbody>
                    </table>
                `)
//line app/vmalert/web.qtpl:178
			}
//line app/vmalert/web.qtpl:178
			qw422016.N().S(`
            </div>
            <br>
        `)
//line app/vmalert/web.qtpl:181
		}
//line app/vmalert/web.qtpl:181
		qw422016.N().S(`

    `)
//line app/vmalert/web.qtpl:183
	} else {
//line app/vmalert/web.qtpl:183
		qw422016.N().S(`
        <div>
            <p>No items...</p>
        </div>
    `)
//line app/vmalert/web.qtpl:187
	}
//line app/vmalert/web.qtpl:187
	qw422016.N().S(`

    `)
//line app/vmalert/web.qtpl:189
	tpl.StreamFooter(qw422016)
//line app/vmalert/web.qtpl:189
	qw422016.N().S(`

`)
//line app/vmalert/web.qtpl:191
}

//line app/vmalert/web.qtpl:191
func WriteListAlerts(qq422016 qtio422016.Writer, pathPrefix string, groupAlerts []GroupAlerts) {
//line app/vmalert/web.qtpl:191
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/web.qtpl:191
	StreamListAlerts(qw422016, pathPrefix, groupAlerts)
//line app/vmalert/web.qtpl:191
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/web.qtpl:191
}

//line app/vmalert/web.qtpl:191
func ListAlerts(pathPrefix string, groupAlerts []GroupAlerts) string {
//line app/vmalert/web.qtpl:191
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/web.qtpl:191
	WriteListAlerts(qb422016, pathPrefix, groupAlerts)
//line app/vmalert/web.qtpl:191
	qs422016 := string(qb422016.B)
//line app/vmalert/web.qtpl:191
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/web.qtpl:191
	return qs422016
//line app/vmalert/web.qtpl:191
}

//line app/vmalert/web.qtpl:193
func StreamListTargets(qw422016 *qt422016.Writer, targets map[notifier.TargetType][]notifier.Target) {
//line app/vmalert/web.qtpl:193
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:194
	tpl.StreamHeader(qw422016, "Notifiers", navItems)
//line app/vmalert/web.qtpl:194
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:195
	if len(targets) > 0 {
//line app/vmalert/web.qtpl:195
		qw422016.N().S(`
         <a class="btn btn-primary" role="button" onclick="collapseAll()">Collapse All</a>
         <a class="btn btn-primary" role="button" onclick="expandAll()">Expand All</a>

         `)
//line app/vmalert/web.qtpl:200
		var keys []string
		for key := range targets {
			keys = append(keys, string(key))
		}
		sort.Strings(keys)

//line app/vmalert/web.qtpl:205
		qw422016.N().S(`

         `)
//line app/vmalert/web.qtpl:207
		for i := range keys {
//line app/vmalert/web.qtpl:207
			qw422016.N().S(`
           `)
//line app/vmalert/web.qtpl:208
			typeK, ns := keys[i], targets[notifier.TargetType(keys[i])]
			count := len(ns)

//line app/vmalert/web.qtpl:210
			qw422016.N().S(`
           <div class="group-heading data-bs-target="rules-`)
//line app/vmalert/web.qtpl:211
			qw422016.E().S(typeK)
//line app/vmalert/web.qtpl:211
			qw422016.N().S(`">
             <span class="anchor" id="notifiers-`)
//line app/vmalert/web.qtpl:212
			qw422016.E().S(typeK)
//line app/vmalert/web.qtpl:212
			qw422016.N().S(`"></span>
             <a href="#notifiers-`)
//line app/vmalert/web.qtpl:213
			qw422016.E().S(typeK)
//line app/vmalert/web.qtpl:213
			qw422016.N().S(`">`)
//line app/vmalert/web.qtpl:213
			qw422016.E().S(typeK)
//line app/vmalert/web.qtpl:213
			qw422016.N().S(` (`)
//line app/vmalert/web.qtpl:213
			qw422016.N().D(count)
//line app/vmalert/web.qtpl:213
			qw422016.N().S(`)</a>
         </div>
         <div class="collapse show" id="notifiers-`)
//line app/vmalert/web.qtpl:215
			qw422016.E().S(typeK)
//line app/vmalert/web.qtpl:215
			qw422016.N().S(`">
             <table class="table table-striped table-hover table-sm">
                 <thead>
                     <tr>
                         <th scope="col">Labels</th>
                         <th scope="col">Address</th>
                     </tr>
                 </thead>
                 <tbody>
                 `)
//line app/vmalert/web.qtpl:224
			for _, n := range ns {
//line app/vmalert/web.qtpl:224
				qw422016.N().S(`
                     <tr>
                         <td>
                              `)
//line app/vmalert/web.qtpl:227
				for _, l := range n.Labels {
//line app/vmalert/web.qtpl:227
					qw422016.N().S(`
                                      <span class="ms-1 badge bg-primary">`)
//line app/vmalert/web.qtpl:228
					qw422016.E().S(l.Name)
//line app/vmalert/web.qtpl:228
					qw422016.N().S(`=`)
//line app/vmalert/web.qtpl:228
					qw422016.E().S(l.Value)
//line app/vmalert/web.qtpl:228
					qw422016.N().S(`</span>
                              `)
//line app/vmalert/web.qtpl:229
				}
//line app/vmalert/web.qtpl:229
				qw422016.N().S(`
                          </td>
                         <td>`)
//line app/vmalert/web.qtpl:231
				qw422016.E().S(n.Notifier.Addr())
//line app/vmalert/web.qtpl:231
				qw422016.N().S(`</td>
                     </tr>
                 `)
//line app/vmalert/web.qtpl:233
			}
//line app/vmalert/web.qtpl:233
			qw422016.N().S(`
              </tbody>
             </table>
         </div>
     `)
//line app/vmalert/web.qtpl:237
		}
//line app/vmalert/web.qtpl:237
		qw422016.N().S(`

    `)
//line app/vmalert/web.qtpl:239
	} else {
//line app/vmalert/web.qtpl:239
		qw422016.N().S(`
        <div>
            <p>No items...</p>
        </div>
    `)
//line app/vmalert/web.qtpl:243
	}
//line app/vmalert/web.qtpl:243
	qw422016.N().S(`

    `)
//line app/vmalert/web.qtpl:245
	tpl.StreamFooter(qw422016)
//line app/vmalert/web.qtpl:245
	qw422016.N().S(`

`)
//line app/vmalert/web.qtpl:247
}

//line app/vmalert/web.qtpl:247
func WriteListTargets(qq422016 qtio422016.Writer, targets map[notifier.TargetType][]notifier.Target) {
//line app/vmalert/web.qtpl:247
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/web.qtpl:247
	StreamListTargets(qw422016, targets)
//line app/vmalert/web.qtpl:247
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/web.qtpl:247
}

//line app/vmalert/web.qtpl:247
func ListTargets(targets map[notifier.TargetType][]notifier.Target) string {
//line app/vmalert/web.qtpl:247
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/web.qtpl:247
	WriteListTargets(qb422016, targets)
//line app/vmalert/web.qtpl:247
	qs422016 := string(qb422016.B)
//line app/vmalert/web.qtpl:247
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/web.qtpl:247
	return qs422016
//line app/vmalert/web.qtpl:247
}

//line app/vmalert/web.qtpl:249
func StreamAlert(qw422016 *qt422016.Writer, pathPrefix string, alert *APIAlert) {
//line app/vmalert/web.qtpl:249
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:250
	tpl.StreamHeader(qw422016, "", navItems)
//line app/vmalert/web.qtpl:250
	qw422016.N().S(`
    `)
//line app/vmalert/web.qtpl:252
	var labelKeys []string
	for k := range alert.Labels {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)

	var annotationKeys []string
	for k := range alert.Annotations {
		annotationKeys = append(annotationKeys, k)
	}
	sort.Strings(annotationKeys)

//line app/vmalert/web.qtpl:263
	qw422016.N().S(`
    <div class="display-6 pb-3 mb-3">`)
//line app/vmalert/web.qtpl:264
	qw422016.E().S(alert.Name)
//line app/vmalert/web.qtpl:264
	qw422016.N().S(`<span class="ms-2 badge `)
//line app/vmalert/web.qtpl:264
	if alert.State == "firing" {
//line app/vmalert/web.qtpl:264
		qw422016.N().S(`bg-danger`)
//line app/vmalert/web.qtpl:264
	} else {
//line app/vmalert/web.qtpl:264
		qw422016.N().S(` bg-warning text-dark`)
//line app/vmalert/web.qtpl:264
	}
//line app/vmalert/web.qtpl:264
	qw422016.N().S(`">`)
//line app/vmalert/web.qtpl:264
	qw422016.E().S(alert.State)
//line app/vmalert/web.qtpl:264
	qw422016.N().S(`</span></div>
    <div class="container border-bottom p-2">
      <div class="row">
        <div class="col-2">
          Active at
        </div>
        <div class="col">
          `)
//line app/vmalert/web.qtpl:271
	qw422016.E().S(alert.ActiveAt.Format("2006-01-02T15:04:05Z07:00"))
//line app/vmalert/web.qtpl:271
	qw422016.N().S(`
        </div>
      </div>
      </div>
    <div class="container border-bottom p-2">
      <div class="row">
        <div class="col-2">
          Expr
        </div>
        <div class="col">
          <code><pre>`)
//line app/vmalert/web.qtpl:281
	qw422016.E().S(alert.Expression)
//line app/vmalert/web.qtpl:281
	qw422016.N().S(`</pre></code>
        </div>
      </div>
    </div>
    <div class="container border-bottom p-2">
      <div class="row">
        <div class="col-2">
          Labels
        </div>
        <div class="col">
           `)
//line app/vmalert/web.qtpl:291
	for _, k := range labelKeys {
//line app/vmalert/web.qtpl:291
		qw422016.N().S(`
                <span class="m-1 badge bg-primary">`)
//line app/vmalert/web.qtpl:292
		qw422016.E().S(k)
//line app/vmalert/web.qtpl:292
		qw422016.N().S(`=`)
//line app/vmalert/web.qtpl:292
		qw422016.E().S(alert.Labels[k])
//line app/vmalert/web.qtpl:292
		qw422016.N().S(`</span>
          `)
//line app/vmalert/web.qtpl:293
	}
//line app/vmalert/web.qtpl:293
	qw422016.N().S(`
        </div>
      </div>
    </div>
    <div class="container border-bottom p-2">
      <div class="row">
        <div class="col-2">
          Annotations
        </div>
        <div class="col">
           `)
//line app/vmalert/web.qtpl:303
	for _, k := range annotationKeys {
//line app/vmalert/web.qtpl:303
		qw422016.N().S(`
                <b>`)
//line app/vmalert/web.qtpl:304
		qw422016.E().S(k)
//line app/vmalert/web.qtpl:304
		qw422016.N().S(`:</b><br>
                <p>`)
//line app/vmalert/web.qtpl:305
		qw422016.E().S(alert.Annotations[k])
//line app/vmalert/web.qtpl:305
		qw422016.N().S(`</p>
          `)
//line app/vmalert/web.qtpl:306
	}
//line app/vmalert/web.qtpl:306
	qw422016.N().S(`
        </div>
      </div>
    </div>
    <div class="container border-bottom p-2">
      <div class="row">
        <div class="col-2">
          Group
        </div>
        <div class="col">
           <a target="_blank" href="`)
//line app/vmalert/web.qtpl:316
	qw422016.E().S(path.Join(pathPrefix, "groups"))
//line app/vmalert/web.qtpl:316
	qw422016.N().S(`#group-`)
//line app/vmalert/web.qtpl:316
	qw422016.E().S(alert.GroupID)
//line app/vmalert/web.qtpl:316
	qw422016.N().S(`">`)
//line app/vmalert/web.qtpl:316
	qw422016.E().S(alert.GroupID)
//line app/vmalert/web.qtpl:316
	qw422016.N().S(`</a>
        </div>
      </div>
    </div>
     <div class="container border-bottom p-2">
      <div class="row">
        <div class="col-2">
          Source link
        </div>
        <div class="col">
           <a target="_blank" href="`)
//line app/vmalert/web.qtpl:326
	qw422016.E().S(alert.SourceLink)
//line app/vmalert/web.qtpl:326
	qw422016.N().S(`">Link</a>
        </div>
      </div>
    </div>
    `)
//line app/vmalert/web.qtpl:330
	tpl.StreamFooter(qw422016)
//line app/vmalert/web.qtpl:330
	qw422016.N().S(`

`)
//line app/vmalert/web.qtpl:332
}

//line app/vmalert/web.qtpl:332
func WriteAlert(qq422016 qtio422016.Writer, pathPrefix string, alert *APIAlert) {
//line app/vmalert/web.qtpl:332
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/web.qtpl:332
	StreamAlert(qw422016, pathPrefix, alert)
//line app/vmalert/web.qtpl:332
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/web.qtpl:332
}

//line app/vmalert/web.qtpl:332
func Alert(pathPrefix string, alert *APIAlert) string {
//line app/vmalert/web.qtpl:332
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/web.qtpl:332
	WriteAlert(qb422016, pathPrefix, alert)
//line app/vmalert/web.qtpl:332
	qs422016 := string(qb422016.B)
//line app/vmalert/web.qtpl:332
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/web.qtpl:332
	return qs422016
//line app/vmalert/web.qtpl:332
}

//line app/vmalert/web.qtpl:334
func streambadgeState(qw422016 *qt422016.Writer, state string) {
//line app/vmalert/web.qtpl:334
	qw422016.N().S(`
`)
//line app/vmalert/web.qtpl:336
	badgeClass := "bg-warning text-dark"
	if state == "firing" {
		badgeClass = "bg-danger"
	}

//line app/vmalert/web.qtpl:340
	qw422016.N().S(`
<span class="badge `)
//line app/vmalert/web.qtpl:341
	qw422016.E().S(badgeClass)
//line app/vmalert/web.qtpl:341
	qw422016.N().S(`">`)
//line app/vmalert/web.qtpl:341
	qw422016.E().S(state)
//line app/vmalert/web.qtpl:341
	qw422016.N().S(`</span>
`)
//line app/vmalert/web.qtpl:342
}

//line app/vmalert/web.qtpl:342
func writebadgeState(qq422016 qtio422016.Writer, state string) {
//line app/vmalert/web.qtpl:342
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/web.qtpl:342
	streambadgeState(qw422016, state)
//line app/vmalert/web.qtpl:342
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/web.qtpl:342
}

//line app/vmalert/web.qtpl:342
func badgeState(state string) string {
//line app/vmalert/web.qtpl:342
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/web.qtpl:342
	writebadgeState(qb422016, state)
//line app/vmalert/web.qtpl:342
	qs422016 := string(qb422016.B)
//line app/vmalert/web.qtpl:342
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/web.qtpl:342
	return qs422016
//line app/vmalert/web.qtpl:342
}

//line app/vmalert/web.qtpl:344
func streambadgeRestored(qw422016 *qt422016.Writer) {
//line app/vmalert/web.qtpl:344
	qw422016.N().S(`
<span class="badge bg-warning text-dark" title="Alert state was restored after the service restart from remote storage">restored</span>
`)
//line app/vmalert/web.qtpl:346
}

//line app/vmalert/web.qtpl:346
func writebadgeRestored(qq422016 qtio422016.Writer) {
//line app/vmalert/web.qtpl:346
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vmalert/web.qtpl:346
	streambadgeRestored(qw422016)
//line app/vmalert/web.qtpl:346
	qt422016.ReleaseWriter(qw422016)
//line app/vmalert/web.qtpl:346
}

//line app/vmalert/web.qtpl:346
func badgeRestored() string {
//line app/vmalert/web.qtpl:346
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vmalert/web.qtpl:346
	writebadgeRestored(qb422016)
//line app/vmalert/web.qtpl:346
	qs422016 := string(qb422016.B)
//line app/vmalert/web.qtpl:346
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vmalert/web.qtpl:346
	return qs422016
//line app/vmalert/web.qtpl:346
}
