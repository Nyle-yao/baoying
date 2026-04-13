# GitHub Pages 部署指南（核心/动态/竞品/驾驶舱）

## 1. 准备仓库
1. 在 GitHub 新建仓库（建议：`baoying-fund-dashboard`）。
2. 把本目录代码推到新仓库根目录。

## 2. 配置 Secrets
在仓库 `Settings -> Secrets and variables -> Actions` 新增：
- `CD_NAMESPACE_NAME`：宝盈基金
- `CD_USER_NAME`：你的账号
- `CD_PASSWORD`：你的密码
- `CD_TARGET`：可选，默认 `https://www.cdollar.cn/leshu-pro/#/e/vq6aKqp5YU`
- `CD_BROWSER_ID`：可选，默认 `09cb220223cb45410e11e84679b83fb6`
- `GH_PAGES_ADMIN_TOKEN`：可选但强烈建议。用于自动启用 Pages（需 `pages:write` + `administration:write`，仓库限定到 `Nyle-yao/baoying`）。

## 3. 启用 Pages
1. 仓库 `Settings -> Pages`
2. `Build and deployment` 选择 `Source: GitHub Actions`
3. 若未手动启用，工作流会尝试用 `GH_PAGES_ADMIN_TOKEN` 自动启用并切到 `build_type=workflow`

## 4. 触发部署
- 手动：`Actions -> Update Dashboards And Deploy Pages -> Run workflow`
- 自动：工作日 16:00（北京时间）自动运行
- 自愈：若部署失败，`Pages Deploy Self-Heal` 会自动重试失败任务（最多 2 次重试）

## 5. 访问地址
- 部署成功后，在 Actions 的 `Deploy to GitHub Pages` 步骤里可见 `page_url`。

## 6. 页面说明
打包后 `docs` 目录会包含：
- `index.html`（核心看板）
- `ops-metrics.html`（动态）
- `competitor-weakness.html`（竞品）
- `fund-detail-cockpit.html`（驾驶舱）
- `v2-pilot.html`（试运营）
- `metrics-doc.html`（指标文档）

> 说明：GitHub Pages 为静态站点，不支持核心看板里的 `/api/update` 按钮实时触发，更新由 Actions 定时执行。
