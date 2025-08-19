class QueryConsole {
    constructor() {
        // Dynamically determine the API URL by replacing query-ui with quixlake
        const currentUrl = window.location.href;
        const baseUrl = currentUrl.replace("query-ui", 'quixlake');
        this.tablesApiUrl = baseUrl + '/tables';
        this.partitionsApiUrl = baseUrl + '/partitions';
        this.partitionInfoApiUrl = baseUrl + '/partition-info';
        this.compactApiUrl = baseUrl + '/compact';
        this.queryApiUrl = baseUrl + '/query';
        this.tasksApiUrl = baseUrl + '/tasks';
        
        // Task management
        this.activeTasks = new Map();
        this.taskEventSources = new Map();
        
        this.initializeElements();
        this.bindEvents();
        this.loadTablesAndIndexes();
        this.loadTasks();  // Load tasks on startup
    }

    initializeElements() {
        this.tablesContainer = document.getElementById('hive-tree');
        this.sqlEditor = document.getElementById('sql-editor');
        this.runQueryBtn = document.getElementById('run-query');
        this.clearQueryBtn = document.getElementById('clear-query');
        this.refreshTreeBtn = document.getElementById('refresh-tree');
        this.compactBtn = document.getElementById('compact-btn');
        this.deleteBtn = document.getElementById('delete-btn');
        this.infoBtn = document.getElementById('info-btn');
        this.repartitionBtn = document.getElementById('repartition-btn');
        this.unionByNameCheckbox = document.getElementById('union-by-name');
        this.explainAnalyzeCheckbox = document.getElementById('explain-analyze');
        this.resultsContainer = document.getElementById('results-container');
        this.explainContainer = document.getElementById('explain-container');
        this.explainContent = document.getElementById('explain-content');
        this.explainTab = document.getElementById('explain-tab');
        this.queryStatus = document.getElementById('query-status');
        this.queryTiming = document.getElementById('query-timing');
        this.selectedFolderDiv = document.getElementById('selected-folder');
        this.selectedPathSpan = document.getElementById('selected-path');
        this.clearSelectionBtn = document.getElementById('clear-selection');
        this.resizer = document.getElementById('resizer');
        this.horizontalResizer = document.getElementById('horizontal-resizer');
        this.sidebar = document.getElementById('sidebar');
        this.selectedTable = '';
        this.selectedPartitionPath = null;
        this.whereConditions = [];
        this.selectedTableElement = null;
        this.selectedPartitionElement = null;
        
        // Waveform elements
        this.waveformContainer = document.getElementById('waveform-container');
        this.xAxisColumn = document.getElementById('x-axis-column');
        this.yAxisColumns = document.getElementById('y-axis-columns');
        this.plotWaveformBtn = document.getElementById('plot-waveform');
        this.waveformChart = null;
        this.queryData = null;
        
        // Modal elements
        this.infoDialog = document.getElementById('info-dialog');
        this.infoDialogClose = document.getElementById('info-dialog-close');
        this.infoClose = document.getElementById('info-close');
        this.infoDialogTitle = document.getElementById('info-dialog-title');
        this.infoContent = document.getElementById('info-content');
        
        this.deleteDialog = document.getElementById('delete-dialog');
        this.deleteDialogClose = document.getElementById('delete-dialog-close');
        this.deleteCancel = document.getElementById('delete-cancel');
        this.deleteExecute = document.getElementById('delete-execute');
        this.deleteTableName = document.getElementById('delete-table-name');
        this.whereClause = document.getElementById('where-clause');
        this.partitionsList = document.getElementById('partitions-list');
        this.whereClauseGroup = document.getElementById('where-clause-group');
        this.partitionsGroup = document.getElementById('partitions-group');
        
        this.compactDialog = document.getElementById('compact-dialog');
        this.compactDialogClose = document.getElementById('compact-dialog-close');
        this.compactCancel = document.getElementById('compact-cancel');
        this.compactExecute = document.getElementById('compact-execute');
        this.compactTableName = document.getElementById('compact-table-name');
        this.targetFileSize = document.getElementById('target-file-size');
        this.tableInfo = document.getElementById('table-info');
        this.tableInfoContent = document.getElementById('table-info-content');
        
        // Repartition dialog elements
        this.repartitionDialog = document.getElementById('repartition-dialog');
        this.repartitionDialogClose = document.getElementById('repartition-dialog-close');
        this.repartitionCancel = document.getElementById('repartition-cancel');
        this.repartitionExecute = document.getElementById('repartition-execute');
        this.repartitionTableName = document.getElementById('repartition-table-name');
        this.repartitionHiveColumns = document.getElementById('repartition-hive-columns');
        this.repartitionTimestampColumn = document.getElementById('repartition-timestamp-column');
        this.repartitionTimestampFormat = document.getElementById('repartition-timestamp-format');
        this.repartitionTargetFileSize = document.getElementById('repartition-target-file-size');
        
        // Task management elements
        this.tasksContainer = document.getElementById('tasks-container');
        this.tasksList = document.getElementById('tasks-list');
        this.refreshTasksBtn = document.getElementById('refresh-tasks');
        
        // Ensure all dialogs are hidden on init
        if (this.repartitionDialog) {
            this.repartitionDialog.style.display = 'none';
        }
        if (this.infoDialog) {
            this.infoDialog.style.display = 'none';
        }
        if (this.deleteDialog) {
            this.deleteDialog.style.display = 'none';
        }
        if (this.compactDialog) {
            this.compactDialog.style.display = 'none';
        }
        
        // Initially disable the action buttons
        this.compactBtn.disabled = true;
        this.deleteBtn.disabled = true;
        this.infoBtn.disabled = true;
        this.repartitionBtn.disabled = true;
    }

    bindEvents() {
        this.runQueryBtn.addEventListener('click', () => this.executeQuery());
        this.clearQueryBtn.addEventListener('click', () => this.clearQuery());
        this.refreshTreeBtn.addEventListener('click', () => this.loadTablesAndIndexes());
        this.compactBtn.addEventListener('click', () => this.showCompactDialog());
        this.deleteBtn.addEventListener('click', () => this.showDeleteDialog());
        this.infoBtn.addEventListener('click', () => this.showInfoDialog());
        this.repartitionBtn.addEventListener('click', () => this.showRepartitionDialog());
        this.clearSelectionBtn.addEventListener('click', () => this.clearSelection());
        
        // Waveform events
        this.plotWaveformBtn.addEventListener('click', () => this.plotWaveform());
        
        // Tab switching
        document.querySelectorAll('.tab-button').forEach(button => {
            button.addEventListener('click', (e) => this.switchTab(e.target.dataset.tab));
        });
        
        // Modal events
        this.infoDialogClose.addEventListener('click', () => this.hideInfoDialog());
        this.infoClose.addEventListener('click', () => this.hideInfoDialog());
        
        this.deleteDialogClose.addEventListener('click', () => this.hideDeleteDialog());
        this.deleteCancel.addEventListener('click', () => this.hideDeleteDialog());
        this.deleteExecute.addEventListener('click', () => this.executeDelete());
        
        this.compactDialogClose.addEventListener('click', () => this.hideCompactDialog());
        this.compactCancel.addEventListener('click', () => this.hideCompactDialog());
        this.compactExecute.addEventListener('click', () => this.executeCompact());
        
        if (this.repartitionDialogClose) {
            this.repartitionDialogClose.addEventListener('click', () => this.hideRepartitionDialog());
        }
        if (this.repartitionCancel) {
            this.repartitionCancel.addEventListener('click', () => this.hideRepartitionDialog());
        }
        if (this.repartitionExecute) {
            this.repartitionExecute.addEventListener('click', () => this.executeRepartition());
        }
        
        // Removed backdrop click handlers - dialogs should only close via buttons
        
        // Delete mode radio button events
        document.querySelectorAll('input[name="delete-mode"]').forEach(radio => {
            radio.addEventListener('change', () => this.toggleDeleteOptions());
        });
        
        // Task management events
        if (this.refreshTasksBtn) {
            this.refreshTasksBtn.addEventListener('click', () => this.loadTasks());
        }
        
        // Enable Ctrl+Enter to run query
        this.sqlEditor.addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                e.preventDefault();
                this.executeQuery();
            }
        });

        // Resizer functionality
        this.setupResizer();
        this.setupHorizontalResizer();
    }

    async loadTablesAndIndexes() {
        try {
            this.tablesContainer.innerHTML = '<div class="loading">Loading tables...</div>';
            
            // Load tables with metadata
            const tablesResponse = await fetch(this.tablesApiUrl + '?include_metadata=true');
            if (!tablesResponse.ok) {
                throw new Error(`HTTP ${tablesResponse.status}: ${tablesResponse.statusText}`);
            }
            
            const tablesData = await tablesResponse.json();
            await this.renderTablesWithPartitions(tablesData.tables);
            
        } catch (error) {
            console.error('Error loading tables and partitions:', error);
            this.tablesContainer.innerHTML = `
                <div class="error-message">
                    Failed to load tables: ${error.message}
                </div>
            `;
        }
    }

    async renderTablesWithPartitions(tables) {
        this.tablesContainer.innerHTML = '';

        if (!tables || tables.length === 0) {
            this.tablesContainer.innerHTML = '<div class="no-results">No tables found</div>';
            return;
        }

        for (const tableData of tables) {
            // Handle both string format (backward compatibility) and object format
            const tableName = typeof tableData === 'string' ? tableData : tableData.name;
            const fileCount = typeof tableData === 'object' ? tableData.file_count : 0;
            const sizeMb = typeof tableData === 'object' ? tableData.total_size_mb : 0;
            
            const tableDiv = document.createElement('div');
            tableDiv.className = 'tree-node table';
            tableDiv.style.display = 'flex';
            tableDiv.style.alignItems = 'center';
            
            // Create expand/collapse button
            const expandBtn = document.createElement('span');
            expandBtn.className = 'expand-btn';
            expandBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M10 17l5-5-5-5v10z"/></svg>';
            expandBtn.style.cursor = 'pointer';
            expandBtn.style.display = 'inline-flex';
            expandBtn.style.alignItems = 'center';
            expandBtn.style.justifyContent = 'center';
            expandBtn.style.transition = 'transform 0.2s';
            
            const labelSpan = document.createElement('span');
            
            // Build label with metadata if available
            let labelHtml = `<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" style="margin-right: 6px; vertical-align: middle;"><path d="M4 6H2v14c0 1.1.9 2 2 2h14v-2H4V6zm16-4H8c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-1 9H9V9h10v2z"/></svg>${tableName}`;
            
            // Add file count and size info if available
            if (fileCount > 0 || sizeMb > 0) {
                const infoparts = [];
                if (fileCount > 0) infoparts.push(`${fileCount} files`);
                if (sizeMb > 0) infoparts.push(`${sizeMb.toFixed(1)} MB`);
                labelHtml += ` <span style="color: #666; font-size: 11px;">(${infoparts.join(', ')})</span>`;
            }
            
            labelSpan.innerHTML = labelHtml;
            labelSpan.style.cursor = 'pointer';
            labelSpan.style.fontWeight = '500';
            labelSpan.style.display = 'flex';
            labelSpan.style.alignItems = 'center';
            labelSpan.style.whiteSpace = 'nowrap';
            
            tableDiv.appendChild(expandBtn);
            tableDiv.appendChild(labelSpan);
            
            // Container for partitions
            const partitionsDiv = document.createElement('div');
            partitionsDiv.className = 'tree-children';
            partitionsDiv.style.display = 'none';
            
            let expanded = false;
            let partitionsLoaded = false;
            
            expandBtn.addEventListener('click', async (e) => {
                e.stopPropagation();
                
                if (!expanded) {
                    if (!partitionsLoaded) {
                        partitionsDiv.innerHTML = '<div class="loading" style="padding: 4px 20px; font-size: 12px;">Loading partitions...</div>';
                        partitionsDiv.style.display = 'block';
                        await this.loadPartitions(tableName, partitionsDiv);
                        partitionsLoaded = true;
                    } else {
                        partitionsDiv.style.display = 'block';
                    }
                    expandBtn.style.transform = 'rotate(90deg)';
                    expanded = true;
                } else {
                    partitionsDiv.style.display = 'none';
                    expandBtn.style.transform = 'rotate(0deg)';
                    expanded = false;
                }
            });
            
            labelSpan.addEventListener('click', (e) => {
                e.stopPropagation();
                this.selectTable(tableName, tableDiv);
            });

            this.tablesContainer.appendChild(tableDiv);
            this.tablesContainer.appendChild(partitionsDiv);
        }
    }

    async loadPartitions(tableName, container, path = '') {
        try {
            // Use lazy loading for root level
            const url = `${this.partitionsApiUrl}?table=${encodeURIComponent(tableName)}&lazy=true&path=${encodeURIComponent(path)}`;
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            // Always clear container to remove loading text or previous content
            container.innerHTML = '';
            
            if (data.partitions && data.partitions.length > 0) {
                this.renderPartitionTreeLazy(tableName, data.partitions, container, path === '' ? 1 : path.split('/').length + 1);
            } else if (path === '') {
                container.innerHTML = '<div style="padding: 4px 32px; font-size: 12px; color: #666;">No partitions found</div>';
            }
        } catch (error) {
            console.error('Error loading partitions:', error);
            if (path === '') {
                container.innerHTML = '<div style="padding: 4px 32px; font-size: 12px; color: #d93025;">Error loading partitions</div>';
            }
        }
    }

    renderPartitionTreeLazy(tableName, items, container, level) {
        if (!items || items.length === 0) return;
        
        items.forEach(item => {
            if (item.type === 'directory') {
                this.renderPartitionNodeLazy(tableName, item, container, level);
            }
        });
    }
    
    renderPartitionNodeLazy(tableName, node, container, level) {
        const nodeDiv = document.createElement('div');
        nodeDiv.className = 'tree-node partition';
        nodeDiv.style.paddingLeft = `${12 + level * 16}px`;
        nodeDiv.style.fontSize = '13px';
        nodeDiv.style.display = 'flex';
        nodeDiv.style.alignItems = 'center';
        
        // Create expand button if has children
        const hasChildren = node.has_children;
        const expandBtn = document.createElement('span');
        if (hasChildren) {
            expandBtn.className = 'expand-btn';
            expandBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M10 17l5-5-5-5v10z"/></svg>';
            expandBtn.style.marginRight = '4px';
            expandBtn.style.cursor = 'pointer';
            expandBtn.style.display = 'inline-flex';
            expandBtn.style.alignItems = 'center';
            expandBtn.style.justifyContent = 'center';
            expandBtn.style.transition = 'transform 0.2s';
        } else {
            expandBtn.innerHTML = '<span style="width: 12px; height: 12px; display: inline-block;"></span>';
            expandBtn.style.marginRight = '4px';
        }
        
        // Partition name and info
        const nameSpan = document.createElement('span');
        nameSpan.style.flexGrow = '1';
        nameSpan.style.display = 'flex';
        nameSpan.style.alignItems = 'center';
        nameSpan.style.gap = '8px';
        
        const folderIcon = '<svg width="14" height="14" viewBox="0 0 24 24" fill="#fbbf24" style="flex-shrink: 0;"><path d="M10 4H4c-1.11 0-2 .89-2 2v12c0 1.11.89 2 2 2h16c1.11 0 2-.89 2-2V8c0-1.11-.89-2-2-2h-8l-2-2z"/></svg>';
        const fileName = node.name;
        const fileCount = node.file_count || node.direct_file_count || 0;
        const sizeMb = node.size_mb || 0;
        
        // Build info text - prioritize direct file count, show size only if available
        let infoText = '';
        if (fileCount > 0) {
            const parts = [];
            parts.push(`${fileCount} files`);
            if (sizeMb > 0) parts.push(`${sizeMb.toFixed(1)}MB`);
            infoText = `<span style="color: #666; font-size: 11px;">(${parts.join(', ')})</span>`;
        }
        
        nameSpan.innerHTML = `
            ${folderIcon}
            <span>${fileName}</span>
            ${infoText}
        `;
        
        // Make nameSpan clickable for selection (like original implementation)
        nameSpan.style.cursor = 'pointer';
        nameSpan.style.color = '#0066cc';
        nameSpan.style.whiteSpace = 'nowrap';
        
        // Add click handler for partition selection
        nameSpan.addEventListener('click', (e) => {
            e.stopPropagation();
            if (node.path) {
                // Generate WHERE clause for partition selection
                this.selectPartition(tableName, node.path, nodeDiv);
            } else {
                // Select just the table
                this.selectTable(tableName, nodeDiv);
            }
        });
        
        nodeDiv.appendChild(expandBtn);
        nodeDiv.appendChild(nameSpan);
        container.appendChild(nodeDiv);
        
        // Container for children (initially empty for lazy loading)
        const childrenDiv = document.createElement('div');
        childrenDiv.style.display = 'none';
        container.appendChild(childrenDiv);
        
        // Track expansion state and loaded state
        let expanded = false;
        let loaded = false;
        
        // Click handler for expansion
        if (hasChildren) {
            expandBtn.addEventListener('click', async (event) => {
                event.stopPropagation();
                
                if (!expanded) {
                    // Expand
                    if (!loaded) {
                        // Lazy load children
                        childrenDiv.innerHTML = '<div style="padding-left: 32px; color: #666; font-size: 12px;">Loading...</div>';
                        childrenDiv.style.display = 'block';
                        
                        try {
                            await this.loadPartitions(tableName, childrenDiv, node.path);
                            loaded = true;
                        } catch (error) {
                            childrenDiv.innerHTML = '<div style="padding-left: 32px; color: #d93025; font-size: 12px;">Error loading partitions</div>';
                        }
                    } else {
                        childrenDiv.style.display = 'block';
                    }
                    expandBtn.style.transform = 'rotate(90deg)';
                    expanded = true;
                } else {
                    // Collapse
                    childrenDiv.style.display = 'none';
                    expandBtn.style.transform = 'rotate(0deg)';
                    expanded = false;
                }
            });
        }
    }
    
    renderPartitionTree(tableName, children, container, level) {
        if (!children || children.length === 0) return;
        
        children.forEach(child => {
            if (child.type === 'directory') {
                this.renderPartitionNode(tableName, child, container, level);
            }
        });
    }

    renderPartitionNode(tableName, node, container, level) {
        const nodeDiv = document.createElement('div');
        nodeDiv.className = 'tree-node partition';
        nodeDiv.style.paddingLeft = `${12 + level * 16}px`;
        nodeDiv.style.fontSize = '13px';
        nodeDiv.style.display = 'flex';
        nodeDiv.style.alignItems = 'center';
        
        // Create expand button if has children
        const hasChildren = node.children && node.children.some(child => child.type === 'directory');
        const expandBtn = document.createElement('span');
        if (hasChildren) {
            expandBtn.className = 'expand-btn';
            expandBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M10 17l5-5-5-5v10z"/></svg>';
            expandBtn.style.marginRight = '4px';
            expandBtn.style.cursor = 'pointer';
            expandBtn.style.display = 'inline-flex';
            expandBtn.style.alignItems = 'center';
            expandBtn.style.justifyContent = 'center';
            expandBtn.style.transition = 'transform 0.2s';
        } else {
            expandBtn.innerHTML = '<span style="width: 12px; height: 12px; display: inline-block;"></span>';
            expandBtn.style.marginRight = '4px';
        }
        
        const labelSpan = document.createElement('span');
        const sizeInfo = node.size_mb ? ` <span style="color: #666; font-size: 11px;">(${node.size_mb.toFixed(1)}MB)</span>` : '';
        labelSpan.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" style="margin-right: 4px; vertical-align: middle; color: #ffa726;"><path d="M10 4H4c-1.11 0-2 .89-2 2v12c0 1.11.89 2 2 2h16c1.11 0 2-.89 2-2V8c0-1.11-.89-2-2-2h-8l-2-2z"/></svg>${node.name}${sizeInfo}`;
        labelSpan.style.cursor = 'pointer';
        labelSpan.style.color = '#0066cc';
        labelSpan.style.display = 'flex';
        labelSpan.style.alignItems = 'center';
        labelSpan.style.whiteSpace = 'nowrap';
        
        nodeDiv.appendChild(expandBtn);
        nodeDiv.appendChild(labelSpan);
        
        // Container for children
        const childrenDiv = document.createElement('div');
        childrenDiv.className = 'tree-children';
        childrenDiv.style.display = 'none';
        
        let expanded = false;
        
        if (hasChildren) {
            expandBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                
                if (!expanded) {
                    // Render children
                    this.renderPartitionTree(tableName, node.children, childrenDiv, level + 1);
                    childrenDiv.style.display = 'block';
                    expandBtn.style.transform = 'rotate(90deg)';
                    expanded = true;
                } else {
                    childrenDiv.style.display = 'none';
                    expandBtn.style.transform = 'rotate(0deg)';
                    expanded = false;
                }
            });
        }
        
        labelSpan.addEventListener('click', (e) => {
            e.stopPropagation();
            if (node.path) {
                // Generate WHERE clause for partition selection
                this.selectPartition(tableName, node.path, nodeDiv);
            } else {
                // Select just the table
                this.selectTable(tableName, nodeDiv);
            }
        });
        
        container.appendChild(nodeDiv);
        if (hasChildren) {
            container.appendChild(childrenDiv);
        }
    }

    insertTableName(tableName) {
        const currentValue = this.sqlEditor.value;
        const cursorPos = this.sqlEditor.selectionStart;
        
        // Insert table name at cursor position
        const beforeCursor = currentValue.substring(0, cursorPos);
        const afterCursor = currentValue.substring(cursorPos);
        
        this.sqlEditor.value = beforeCursor + tableName + afterCursor;
        this.sqlEditor.focus();
        
        // Position cursor after inserted text
        const newCursorPos = cursorPos + tableName.length;
        this.sqlEditor.setSelectionRange(newCursorPos, newCursorPos);
    }

    // DEPRECATED: Backend now handles union_by_name parameter
    // addUnionByNameToQuery(query) {
    //     // Transform FROM table_name to read_parquet with union_by_name
    //     const fromPattern = /FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/gi;
    //     
    //     return query.replace(fromPattern, (match, tableName) => {
    //         // Skip if it's already a function call
    //         if (match.toLowerCase().includes('read_parquet') || match.toLowerCase().includes('delta_scan')) {
    //             return match;
    //         }
    //         
    //         // Convert to direct parquet read with union_by_name
    //         return `FROM read_parquet('state/data/${tableName}/**/*.parquet', union_by_name=true, hive_partitioning=true)`;
    //     });
    // }

    async executeQuery() {
        let query = this.sqlEditor.value.trim();
        
        if (!query) {
            this.showStatus('Please enter a query', 'error');
            return;
        }

        // Replace :selected: with the selected table name
        if (query.includes(':selected:') && this.selectedTable) {
            query = query.replace(/:selected:/g, this.selectedTable);
            
            // If a partition is selected, also add the WHERE clause
            if (this.selectedPartitionPath) {
                this.addPartitionWhereClause(this.selectedPartitionPath);
                // Re-read the updated query for execution
                query = this.sqlEditor.value.trim();
            }
        }

        // Don't modify query here - let backend handle union_by_name
        // The backend will apply union_by_name parameter when replacing table names


        this.runQueryBtn.disabled = true;
        this.runQueryBtn.textContent = 'Running...';
        this.showStatus('Executing query...', '');
        this.queryTiming.textContent = '';

        const startTime = performance.now();

        try {
            const networkStartTime = performance.now();
            
            // Build query URL with parameters
            let queryUrl = this.queryApiUrl;
            const params = [];
            
            if (this.explainAnalyzeCheckbox.checked) {
                params.push('explain=true');
            }
            
            if (this.unionByNameCheckbox.checked) {
                params.push('union_by_name=true');
            }
            
            if (params.length > 0) {
                queryUrl += '?' + params.join('&');
            }
            
            const response = await fetch(queryUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain',
                },
                body: query
            });

            const responseReceivedTime = performance.now();

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

            const dataTransferStartTime = performance.now();
            
            // Check if response is JSON (when explain is enabled)
            const contentType = response.headers.get('content-type');
            let result, explainOutput;
            
            if (contentType && contentType.includes('application/json')) {
                // JSON response with explain
                const jsonResult = await response.json();
                result = jsonResult.data;
                explainOutput = jsonResult.explain;
            } else {
                // Plain CSV response
                result = await response.text();
            }
            
            const endTime = performance.now();
            
            // Calculate timing breakdown
            const totalTime = endTime - startTime;
            const serverTime = responseReceivedTime - networkStartTime; // Time until response headers received
            const dataTransferTime = endTime - dataTransferStartTime; // Time to read response body
            
            this.displayResults(result);
            
            // Display explain output if available
            if (explainOutput) {
                this.displayExplainOutput(explainOutput);
                // Show explain tab
                this.switchTab('explain');
                this.explainTab.style.display = 'block';
            } else {
                // Hide explain tab when not using explain
                this.explainTab.style.display = 'none';
                this.switchTab('results');
            }
            
            this.showStatus('Query completed successfully', 'success');
            this.showQueryTiming(totalTime, serverTime, dataTransferTime, result);

        } catch (error) {
            console.error('Query error:', error);
            this.displayError(error.message);
            this.showStatus('Query failed', 'error');
            this.queryTiming.textContent = '';
        } finally {
            this.runQueryBtn.disabled = false;
            this.runQueryBtn.textContent = 'Run Query';
        }
    }

    displayResults(result) {
        // Check if result looks like CSV (has comma separators and newlines)
        if (result.includes(',') && result.includes('\n')) {
            this.displayCsvTable(result);
        } else {
            // Display as preformatted text
            this.resultsContainer.innerHTML = `
                <div class="results-pre">${this.escapeHtml(result)}</div>
            `;
        }
    }

    displayCsvTable(result) {
        const lines = result.trim().split('\n');
        
        if (lines.length < 2) {
            this.resultsContainer.innerHTML = `<div class="results-pre">${this.escapeHtml(result)}</div>`;
            return;
        }

        // Parse CSV - first line is headers
        const headerLine = lines[0];
        const headers = this.parseCsvLine(headerLine);

        // Store data for waveform visualization
        this.queryData = {
            headers: headers,
            rows: []
        };

        const table = document.createElement('table');
        table.className = 'results-table';

        // Create header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Create body
        const tbody = document.createElement('tbody');
        
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i].trim();
            if (line) {
                const cells = this.parseCsvLine(line);
                if (cells.length === headers.length) {
                    // Store row data
                    this.queryData.rows.push(cells);
                    
                    const row = document.createElement('tr');
                    cells.forEach(cell => {
                        const td = document.createElement('td');
                        td.textContent = cell;
                        row.appendChild(td);
                    });
                    tbody.appendChild(row);
                }
            }
        }

        table.appendChild(tbody);
        this.resultsContainer.innerHTML = '';
        this.resultsContainer.appendChild(table);
        
        // Update waveform column selectors
        this.updateWaveformColumns();
    }

    parseCsvLine(line) {
        const result = [];
        let current = '';
        let inQuotes = false;
        
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            
            if (char === '"' && (i === 0 || line[i-1] === ',')) {
                inQuotes = true;
            } else if (char === '"' && inQuotes && (i === line.length - 1 || line[i+1] === ',')) {
                inQuotes = false;
            } else if (char === ',' && !inQuotes) {
                result.push(current.trim());
                current = '';
            } else {
                current += char;
            }
        }
        
        result.push(current.trim());
        return result;
    }

    displayError(error) {
        this.resultsContainer.innerHTML = `
            <div class="error-message">
                <strong>Error:</strong> ${this.escapeHtml(error)}
            </div>
        `;
    }

    selectTable(tableName, tableElement = null) {
        // Clear previous selections
        this.clearTreeSelection();
        
        this.selectedTable = tableName;
        this.selectedPartitionPath = null;
        this.selectedTableElement = tableElement;
        this.selectedPartitionElement = null;
        
        // Highlight the selected table in the tree
        if (tableElement) {
            tableElement.classList.add('selected');
        }
        
        this.compactBtn.disabled = false;
        this.deleteBtn.disabled = false;
        this.infoBtn.disabled = false;
        this.repartitionBtn.disabled = false;
        console.log('Selected table:', tableName);
        
        // Replace FROM clauses in the current query immediately
        this.replaceFromClauses();
    }

    selectPartition(tableName, partitionPath, partitionElement = null) {
        // Clear previous selections
        this.clearTreeSelection();
        
        this.selectedTable = tableName;
        this.selectedPartitionPath = partitionPath;
        this.selectedTableElement = null;
        this.selectedPartitionElement = partitionElement;
        
        // Highlight the selected partition in the tree
        if (partitionElement) {
            partitionElement.classList.add('selected');
        }
        
        this.compactBtn.disabled = false;
        this.deleteBtn.disabled = false;
        this.infoBtn.disabled = false;
        this.repartitionBtn.disabled = false;
        console.log('Selected partition:', tableName, partitionPath);
        
        // Replace FROM clauses and add WHERE clauses
        this.replaceFromClauses();
        this.addPartitionWhereClause(partitionPath);
    }


    addPartitionWhereClause(partitionPath) {
        // Parse partition path like "year=2025/month=07/day=29/machine=3D_PRINTER_0/experiment_name=test"
        const partitionConditions = partitionPath.split('/').map(part => {
            const [key, value] = part.split('=');
            // Handle different value types - strings should be quoted unless they're numbers
            const quotedValue = isNaN(value) && value !== 'null' ? `'${value}'` : value;
            return `${key} = ${quotedValue}`;
        });
        
        const partitionWhereClause = partitionConditions.join(' AND ');
        let currentQuery = this.sqlEditor.value;
        
        // Split the query into logical parts
        const parts = this.parseQuery(currentQuery);
        
        // Remove any existing partition conditions from existing WHERE clause
        if (parts.where) {
            const partitionKeys = partitionPath.split('/').map(part => part.split('=')[0]);
            const partitionPattern = new RegExp(`(^|\\s+AND\\s+)(${partitionKeys.join('|')})\\s*=\\s*'?[^'\\s,;)]+('|\\s|,|;|\\)|$)`, 'gi');
            parts.where = parts.where.replace(partitionPattern, '').replace(/^\s*(AND\s+)?/, '').replace(/\s*(AND\s+)?$/, '');
        }
        
        // Construct the new WHERE clause
        if (parts.where && parts.where.trim()) {
            parts.where = partitionWhereClause + ' AND ' + parts.where;
        } else {
            parts.where = partitionWhereClause;
        }
        
        // Reconstruct the query
        const newQuery = this.reconstructQuery(parts);
        this.sqlEditor.value = newQuery;
        console.log('Added partition WHERE clause:', partitionWhereClause);
    }

    parseQuery(query) {
        const parts = {
            select: '',
            from: '',
            where: '',
            groupBy: '',
            having: '',
            orderBy: '',
            limit: '',
            semicolon: false
        };
        
        // Check for semicolon at the end
        parts.semicolon = query.trim().endsWith(';');
        if (parts.semicolon) {
            query = query.trim().slice(0, -1);
        }
        
        // Split by keywords (case insensitive)
        const keywords = ['FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT'];
        let remaining = query;
        let currentPart = 'select';
        
        for (const keyword of keywords) {
            const regex = new RegExp(`\\s+${keyword}\\s+`, 'i');
            const match = remaining.match(regex);
            
            if (match) {
                // Found the keyword
                const beforeKeyword = remaining.substring(0, match.index);
                const afterKeyword = remaining.substring(match.index + match[0].length);
                
                parts[currentPart] = beforeKeyword.trim();
                remaining = afterKeyword;
                currentPart = keyword.toLowerCase().replace(' ', '');
            }
        }
        
        // Whatever is left goes to the current part
        parts[currentPart] = remaining.trim();
        
        return parts;
    }

    reconstructQuery(parts) {
        let query = parts.select || 'SELECT *';
        
        if (parts.from) {
            query += '\nFROM ' + parts.from;
        }
        
        if (parts.where) {
            query += '\nWHERE ' + parts.where;
        }
        
        if (parts.groupBy) {
            query += '\nGROUP BY ' + parts.groupBy;
        }
        
        if (parts.having) {
            query += '\nHAVING ' + parts.having;
        }
        
        if (parts.orderBy) {
            query += '\nORDER BY ' + parts.orderBy;
        }
        
        if (parts.limit) {
            query += '\nLIMIT ' + parts.limit;
        }
        
        if (parts.semicolon) {
            query += ';';
        }
        
        return query;
    }

    clearTreeSelection() {
        // Remove selected class from all tree nodes
        const selectedNodes = document.querySelectorAll('.tree-node.selected');
        selectedNodes.forEach(node => node.classList.remove('selected'));
    }

    clearSelection() {
        this.clearTreeSelection();
        this.selectedTable = '';
        this.selectedPartitionPath = null;
        this.whereConditions = [];
        this.selectedTableElement = null;
        this.selectedPartitionElement = null;
        this.compactBtn.disabled = true;
        this.deleteBtn.disabled = true;
        this.infoBtn.disabled = true;
        this.repartitionBtn.disabled = true;
    }

    async showInfoDialog() {
        if (!this.selectedTable) {
            this.showStatus('Please select a table to view information', 'error');
            return;
        }

        // Set dialog title and show modal immediately
        if (this.selectedPartitionPath) {
            this.infoDialogTitle.textContent = `Partition Information: ${this.selectedTable}/${this.selectedPartitionPath}`;
        } else {
            this.infoDialogTitle.textContent = `Table Information: ${this.selectedTable}`;
        }
        
        this.infoDialog.style.display = 'flex';
        document.body.style.overflow = 'hidden';
        
        // Load information in background
        this.loadInfoContent();
    }

    async loadInfoContent() {
        try {
            this.infoContent.innerHTML = '<div class="loading">Loading information...</div>';
            
            // If a partition is selected, only load info for that partition
            // Don't load table-level partition info which scans entire table
            if (this.selectedPartitionPath) {
                // Only load partition details and schema for selected partition
                const [partitionData, schemaData] = await Promise.all([
                    this.loadPartitionDetails(this.selectedTable),
                    this.loadTableSchema()
                ]);
                
                // Pass null for partitionInfo since we're looking at a specific partition
                this.displayDetailedInfo(partitionData, schemaData, null);
            } else {
                // Load full table info only when looking at the table level
                const [partitionData, schemaData, partitionInfo] = await Promise.all([
                    this.loadPartitionDetails(this.selectedTable),
                    this.loadTableSchema(),
                    this.loadPartitionInfo(this.selectedTable)
                ]);
                
                // Generate comprehensive info content
                this.displayDetailedInfo(partitionData, schemaData, partitionInfo);
            }
        } catch (error) {
            console.error('Error loading info content:', error);
            this.infoContent.innerHTML = `
                <div style="color: #d93025; padding: 20px; text-align: center;">
                    <strong>Error loading information</strong><br>
                    <small>${error.message}</small>
                </div>
            `;
        }
    }

    async loadTableSchema() {
        try {
            // Use the new schema endpoint to get schema without scanning data
            const schemaUrl = `${this.queryApiUrl.replace('/query', '/schema')}?table=${encodeURIComponent(this.selectedTable)}`;
            const response = await fetch(schemaUrl);
            
            if (response.ok) {
                const schemaData = await response.json();
                if (schemaData.columns) {
                    // Extract column names and types
                    const columns = schemaData.columns.map(col => col.name);
                    const types = schemaData.columns.map(col => col.type);
                    return { 
                        columns: columns, 
                        sampleData: types,  // Use types as sample data for display
                        columnInfo: schemaData.columns
                    };
                }
            }
        } catch (error) {
            console.log('Could not load table schema:', error);
        }
        
        return { columns: [], sampleData: [], columnInfo: [] };
    }

    displayDetailedInfo(partitionData, schemaData, partitionInfo) {
        // Handle both lazy-loaded and full tree formats
        const partitions = partitionData?.partitions || [];
        let summary = partitionData?.partition_tree?.summary || {};
        
        // Calculate summary from lazy-loaded data
        if (partitions.length > 0) {
            // Always calculate from partitions for lazy-loaded data
            const calculatedTotalFiles = partitions.reduce((sum, p) => sum + (p.file_count || 0), 0);
            const calculatedTotalSize = partitions.reduce((sum, p) => sum + (p.size_mb || 0), 0);
            const calculatedIsPartitioned = partitions.some(p => p.has_children);
            const calculatedPartitionCount = partitions.filter(p => p.has_children).length;
            
            // Use calculated values if summary is empty or update with calculated values
            summary = {
                total_files: summary.total_files || calculatedTotalFiles,
                total_size_mb: summary.total_size_mb || calculatedTotalSize,
                is_partitioned: summary.is_partitioned !== undefined ? summary.is_partitioned : calculatedIsPartitioned,
                partition_count: summary.partition_count || calculatedPartitionCount,
                direct_files: summary.direct_files
            };
        }
        
        const isPartitioned = summary?.is_partitioned || partitions.some(p => p.has_children);
        const hiveColumns = this.detectHiveColumns(partitionData) || this.detectHiveColumnsFromLazy(partitions);
        
        // Check for partition structure inconsistencies (only if we have partitionInfo)
        const hasInconsistentPartitions = partitionInfo && partitionInfo.partition_columns && 
                                         hiveColumns.length > 0 && 
                                         !this.arraysEqual(partitionInfo.partition_columns, hiveColumns);
        
        let infoHtml = `
            <div style="display: grid; gap: 20px;">
                <!-- Overview Section -->
                <div style="background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); border-radius: 12px; padding: 20px; border: 1px solid #dee2e6;">
                    <h4 style="margin: 0 0 16px 0; color: #343a40; display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 20px;">📊</span> Overview
                    </h4>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px;">
                        <div style="grid-column: 1 / -1;">
                            <strong>${this.selectedPartitionPath ? 'Partition Path' : 'Table Name'}:</strong><br>
                            <code style="background: #f1f3f4; padding: 4px 8px; border-radius: 4px; display: inline-block; max-width: 100%; overflow-x: auto; white-space: nowrap; font-size: 12px;">${this.selectedPartitionPath ? `${this.selectedTable}/${this.selectedPartitionPath}` : this.selectedTable}</code>
                        </div>
                        <div>
                            <strong>${this.selectedPartitionPath ? 'Partition Size' : 'Total Size'}:</strong><br>
                            ${summary.total_size_mb !== undefined && summary.total_size_mb !== null ? summary.total_size_mb.toFixed(1) + ' MB' : 'Unknown'}
                        </div>
                        <div>
                            <strong>${this.selectedPartitionPath ? 'Files in Partition' : 'Total Files'}:</strong><br>
                            ${summary.total_files || 0}
                        </div>
                    </div>
                </div>

                <!-- Partitioning Section -->
                <div style="background: linear-gradient(135deg, ${hasInconsistentPartitions ? '#fee2e2' : (isPartitioned ? '#e8f5e8' : '#fff3cd')} 0%, ${hasInconsistentPartitions ? '#fca5a5' : (isPartitioned ? '#d4edda' : '#ffeaa7')} 100%); border-radius: 12px; padding: 20px; border: 1px solid ${hasInconsistentPartitions ? '#f87171' : (isPartitioned ? '#c3e6cb' : '#fad5a5')};">
                    <h4 style="margin: 0 0 16px 0; color: #343a40; display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 20px;">${hasInconsistentPartitions ? '⚠️' : (isPartitioned ? '🗂️' : '📄')}</span> Partitioning ${hasInconsistentPartitions ? '(Inconsistent Structure)' : ''}
                    </h4>
                    ${hasInconsistentPartitions ? `
                        <div style="background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 12px; margin-bottom: 16px;">
                            <strong style="color: #dc2626;">⚠️ Partition Structure Issue Detected!</strong><br>
                            <p style="margin: 8px 0; color: #7f1d1d; font-size: 14px;">
                                This table has inconsistent partition structures. This can cause query failures and prevents optimal performance.
                            </p>
                            <div style="margin-top: 8px;">
                                <strong style="color: #dc2626;">Expected:</strong> ${partitionInfo.partition_columns.map(col => `<code style="background: rgba(255,255,255,0.8); padding: 2px 4px; border-radius: 3px;">${col}</code>`).join(', ')}<br>
                                <strong style="color: #dc2626;">Found:</strong> ${hiveColumns.map(col => `<code style="background: rgba(255,255,255,0.8); padding: 2px 4px; border-radius: 3px;">${col}</code>`).join(', ')}
                            </div>
                            <p style="margin: 8px 0 0 0; color: #7f1d1d; font-size: 13px;">
                                <strong>Solution:</strong> Use the "Repartition" feature to standardize the partition structure.
                            </p>
                        </div>
                    ` : ''}
                    <div>
                        <strong>Partitioned:</strong> ${isPartitioned ? '✅ Yes' : '❌ No'}<br>
                        ${isPartitioned ? `
                            <strong>Partitions:</strong> ${summary.partition_count || 0}<br>
                            ${partitionInfo && partitionInfo.partition_columns ? `<strong>Current Structure:</strong> ${partitionInfo.partition_columns.map(col => `<code style="background: rgba(255,255,255,0.7); padding: 2px 6px; border-radius: 4px;">${col}</code>`).join(', ')}<br>` : ''}
                        ` : ''}
                        ${summary?.direct_files > 0 ? `<strong>Direct Files:</strong> ${summary.direct_files}<br>` : ''}
                        ${this.selectedPartitionPath ? `
                            <div style="margin-top: 12px; padding: 8px; background: rgba(255,255,255,0.5); border-radius: 6px;">
                                <strong>Selected Partition:</strong><br>
                                <code>${this.selectedPartitionPath}</code>
                            </div>
                        ` : ''}
                    </div>
                </div>

                <!-- Schema Section -->
                ${schemaData.columns.length > 0 ? `
                <div style="background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); border-radius: 12px; padding: 20px; border: 1px solid #90caf9;">
                    <h4 style="margin: 0 0 16px 0; color: #343a40; display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 20px;">🏗️</span> Schema
                    </h4>
                    <div>
                        <strong>Columns (${schemaData.columns.length}):</strong><br>
                        <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 8px; margin-top: 8px;">
                            ${schemaData.columns.map((col, i) => `
                                <div style="background: rgba(255,255,255,0.7); padding: 6px 10px; border-radius: 6px; font-size: 13px;">
                                    <code>${col}</code>
                                    ${schemaData.sampleData[i] ? `<br><small style="color: #6c757d;">e.g., ${schemaData.sampleData[i].length > 20 ? schemaData.sampleData[i].substring(0, 20) + '...' : schemaData.sampleData[i]}</small>` : ''}
                                </div>
                            `).join('')}
                        </div>
                    </div>
                </div>
                ` : ''}

                ${this.selectedPartitionPath && partitions.length > 0 ? `
                <!-- Partition Contents Section -->
                <div style="background: linear-gradient(135deg, #f0f4f8 0%, #d9e2ec 100%); border-radius: 12px; padding: 20px; border: 1px solid #cbd5e0;">
                    <h4 style="margin: 0 0 16px 0; color: #343a40; display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 20px;">📁</span> Partition Contents
                    </h4>
                    <div style="max-height: 200px; overflow-y: auto; background: rgba(255,255,255,0.5); padding: 12px; border-radius: 6px;">
                        ${partitions.length === 0 ? 
                            '<div style="color: #6c757d; text-align: center;">No items in this partition</div>' :
                            '<ul style="margin: 0; padding-left: 20px;">' +
                            partitions.slice(0, 20).map(p => `
                                <li style="margin: 4px 0;">
                                    ${p.type === 'directory' ? '📁' : '📄'} <code>${p.name}</code>
                                    ${p.file_count > 0 ? ` (${p.file_count} files)` : ''}
                                    ${p.size_mb > 0 ? ` - ${p.size_mb.toFixed(2)} MB` : ''}
                                </li>
                            `).join('') +
                            (partitions.length > 20 ? `<li style="color: #6c757d;">... and ${partitions.length - 20} more items</li>` : '') +
                            '</ul>'
                        }
                    </div>
                </div>
                ` : ''}

                <!-- Actions Section -->
                <div style="background: linear-gradient(135deg, #f3e5f5 0%, #e1bee7 100%); border-radius: 12px; padding: 20px; border: 1px solid #ce93d8;">
                    <h4 style="margin: 0 0 16px 0; color: #343a40; display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 20px;">⚡</span> Available Actions
                    </h4>
                    <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                        <span style="background: rgba(255,255,255,0.7); padding: 4px 8px; border-radius: 4px; font-size: 12px;">🗑️ Delete Data</span>
                        <span style="background: rgba(255,255,255,0.7); padding: 4px 8px; border-radius: 4px; font-size: 12px;">🗜️ Compact Table</span>
                        <span style="background: rgba(255,255,255,0.7); padding: 4px 8px; border-radius: 4px; font-size: 12px;">🔍 Query Data</span>
                        <span style="background: rgba(255,255,255,0.7); padding: 4px 8px; border-radius: 4px; font-size: 12px;">📈 Analyze Structure</span>
                    </div>
                </div>
            </div>
        `;
        
        this.infoContent.innerHTML = infoHtml;
    }

    hideInfoDialog() {
        this.infoDialog.style.display = 'none';
        document.body.style.overflow = '';
    }

    showDeleteDialog() {
        if (!this.selectedTable) {
            this.showStatus('Please select a table to delete from', 'error');
            return;
        }

        // Reset form to defaults
        this.deleteTableName.value = this.selectedTable;
        document.querySelector('input[name="delete-mode"][value="conditional"]').checked = true;
        this.whereClause.value = '';
        this.partitionsList.value = '';
        
        // Pre-fill partition path if partition is selected
        if (this.selectedPartitionPath) {
            this.partitionsList.value = this.selectedPartitionPath;
        }
        
        this.toggleDeleteOptions();
        
        // Show modal
        this.deleteDialog.style.display = 'flex';
        document.body.style.overflow = 'hidden';
    }

    hideDeleteDialog() {
        this.deleteDialog.style.display = 'none';
        document.body.style.overflow = '';
    }

    toggleDeleteOptions() {
        const selectedMode = document.querySelector('input[name="delete-mode"]:checked').value;
        
        this.whereClauseGroup.style.display = selectedMode === 'conditional' ? 'block' : 'none';
        this.partitionsGroup.style.display = selectedMode === 'partition' ? 'block' : 'none';
    }

    async executeDelete() {
        const selectedMode = document.querySelector('input[name="delete-mode"]:checked').value;
        const originalText = this.deleteExecute.innerHTML;
        
        this.deleteExecute.disabled = true;
        this.deleteExecute.innerHTML = 'Deleting...';
        
        try {
            // Build parameters based on delete mode
            const params = new URLSearchParams({
                table: this.selectedTable
            });

            if (selectedMode === 'conditional') {
                const whereClause = this.whereClause.value.trim();
                if (!whereClause) {
                    throw new Error('WHERE clause is required for conditional delete');
                }
                params.append('where', whereClause);
            } else if (selectedMode === 'truncate') {
                // No additional parameters needed - will delete all data but keep table structure
            } else if (selectedMode === 'partition') {
                const partitions = this.partitionsList.value.trim();
                if (!partitions) {
                    throw new Error('Partition list is required for partition delete');
                }
                params.append('partitions', partitions);
            } else if (selectedMode === 'drop') {
                params.append('delete_table', 'true');
            }

            const response = await fetch(`${this.queryApiUrl.replace('/query', '/delete')}?${params.toString()}`, {
                method: 'DELETE'
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

            const result = await response.json();
            console.log('Delete result:', result);
            
            // Show success message with stats
            const stats = result.deletion_result || {};
            let message = `✅ Successfully deleted data from ${this.selectedTable}`;
            
            if (stats.deletion_mode === 'entire_table') {
                message = `✅ Successfully dropped table ${this.selectedTable}`;
            } else if (stats.rows_deleted === 'all') {
                message = `✅ Successfully truncated table ${this.selectedTable} (${stats.files_deleted || 0} files deleted)`;
            } else if (stats.partitions_deleted > 0) {
                message = `✅ Successfully deleted ${stats.partitions_deleted} partitions from ${this.selectedTable}`;
            } else if (stats.rows_deleted > 0) {
                message = `✅ Successfully deleted ${stats.rows_deleted.toLocaleString()} rows from ${this.selectedTable}`;
            }
            
            this.showStatus(message, 'success');
            this.displayDeleteResults(stats);
            
            // Hide dialog and refresh tree
            this.hideDeleteDialog();
            if (stats.deletion_mode === 'entire_table') {
                this.clearSelection(); // Clear selection if table was dropped
            }
            setTimeout(() => {
                this.loadTablesAndIndexes();
            }, 1000);

        } catch (error) {
            console.error('Delete error:', error);
            this.showStatus(`Delete failed: ${error.message}`, 'error');
        } finally {
            this.deleteExecute.innerHTML = originalText;
            this.deleteExecute.disabled = false;
        }
    }

    displayDeleteResults(stats) {
        const modeLabels = {
            'conditional': 'Conditional Delete',
            'truncate_table': 'Table Truncate',
            'partition_based': 'Partition Delete',
            'entire_table': 'Table Drop'
        };
        
        this.resultsContainer.innerHTML = `
            <div style="padding: 20px; font-family: 'Segoe UI', sans-serif;">
                <h3 style="color: #d93025; margin-bottom: 16px; display: flex; align-items: center;">
                    <span style="font-size: 20px; margin-right: 8px;">🗑️</span>
                    Delete Operation Completed
                </h3>
                
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 20px;">
                    ${stats.rows_deleted !== undefined && stats.rows_deleted !== 'all' ? `
                    <div style="background: #f8f9fa; padding: 16px; border-radius: 8px; border-left: 4px solid #d93025;">
                        <div style="font-size: 24px; font-weight: bold; color: #d93025;">${stats.rows_deleted.toLocaleString()}</div>
                        <div style="color: #5f6368; font-size: 14px;">Rows Deleted</div>
                    </div>
                    ` : ''}
                    
                    ${stats.partitions_deleted > 0 ? `
                    <div style="background: #f8f9fa; padding: 16px; border-radius: 8px; border-left: 4px solid #ff9800;">
                        <div style="font-size: 24px; font-weight: bold; color: #f57c00;">${stats.partitions_deleted}</div>
                        <div style="color: #5f6368; font-size: 14px;">Partitions Deleted</div>
                    </div>
                    ` : ''}
                    
                    ${stats.files_deleted > 0 ? `
                    <div style="background: #f8f9fa; padding: 16px; border-radius: 8px; border-left: 4px solid #9c27b0;">
                        <div style="font-size: 24px; font-weight: bold; color: #7b1fa2;">${stats.files_deleted}</div>
                        <div style="color: #5f6368; font-size: 14px;">Files Deleted</div>
                    </div>
                    ` : ''}
                    
                    ${stats.size_deleted_mb > 0 ? `
                    <div style="background: #f8f9fa; padding: 16px; border-radius: 8px; border-left: 4px solid #34a853;">
                        <div style="font-size: 24px; font-weight: bold; color: #137333;">${stats.size_deleted_mb.toFixed(1)} MB</div>
                        <div style="color: #5f6368; font-size: 14px;">Data Deleted</div>
                    </div>
                    ` : ''}
                </div>
                
                <div style="color: #5f6368; font-size: 14px; line-height: 1.4;">
                    <strong>Table:</strong> ${this.selectedTable}<br>
                    <strong>Delete Mode:</strong> ${modeLabels[stats.deletion_mode] || stats.deletion_mode}<br>
                    ${stats.deletion_mode === 'entire_table' ? '<strong style="color: #d93025;">⚠️ Table has been completely removed</strong>' : ''}
                </div>
            </div>
        `;
    }

async showCompactDialog() {
        if (!this.selectedTable) {
            this.showStatus('Please select a table to compact', 'error');
            return;
        }

        // Set table name and show modal immediately
        this.compactTableName.value = this.selectedTable;
        this.compactDialog.style.display = 'flex';
        document.body.style.overflow = 'hidden';
        
        // Set default file size
        this.targetFileSize.value = '128';
        
        // Load table info in background
        this.loadCompactTableInfo();
    }

    async loadCompactTableInfo() {
        try {
            // Load basic table information
            const partitionData = await this.loadPartitionDetails(this.selectedTable);
            this.displayTableInfo(partitionData);
        } catch (error) {
            console.log('Could not load table info:', error);
        }
    }

    displayTableInfo(partitionData) {
        if (!partitionData?.partition_tree?.summary) {
            this.tableInfo.style.display = 'none';
            return;
        }
        
        const summary = partitionData.partition_tree.summary;
        const isPartitioned = summary.is_partitioned;
        
        let infoHtml = `
            <strong>📊 Current Table Status:</strong><br>
            • <strong>Total Size:</strong> ${summary.total_size_mb ? summary.total_size_mb.toFixed(1) + ' MB' : 'Unknown'}<br>
            • <strong>Total Files:</strong> ${summary.total_files || 0}<br>
            • <strong>Partitioned:</strong> ${isPartitioned ? '✅ Yes' : '❌ No'}
        `;
        
        if (isPartitioned) {
            infoHtml += `<br>• <strong>Partitions:</strong> ${summary.partition_count || 0}`;
            const hiveColumns = this.detectHiveColumns(partitionData);
            if (hiveColumns.length > 0) {
                infoHtml += `<br>• <strong>Existing Structure:</strong> ${hiveColumns.join(', ')}`;
            }
        }
        
        if (summary.direct_files > 0) {
            infoHtml += `<br>• <strong>Direct Files:</strong> ${summary.direct_files}`;
        }
        
        this.tableInfoContent.innerHTML = infoHtml;
        this.tableInfo.style.display = 'block';
    }

    detectHiveColumnsFromLazy(partitions) {
        // Extract Hive columns from lazy-loaded partition data
        if (!partitions || partitions.length === 0) {
            return [];
        }
        
        const columns = new Set();
        for (const partition of partitions) {
            if (partition.name && partition.name.includes('=')) {
                const [columnName] = partition.name.split('=');
                if (columnName) {
                    columns.add(columnName);
                }
            }
        }
        
        return Array.from(columns);
    }
    
    detectHiveColumns(partitionData) {
        if (!partitionData?.partition_tree?.tree?.children) {
            return [];
        }
        
        // Traverse the partition tree level by level to maintain hierarchy order
        const columnsByLevel = [];
        
        const analyzeLevel = (children, level = 0) => {
            if (!children || children.length === 0) return;
            
            // Initialize level array if needed
            if (!columnsByLevel[level]) {
                columnsByLevel[level] = new Set();
            }
            
            for (const child of children) {
                if (child.type === 'directory' && child.name && child.name.includes('=')) {
                    const [columnName] = child.name.split('=');
                    if (columnName) {
                        columnsByLevel[level].add(columnName);
                    }
                    
                    // Recursively analyze children
                    if (child.children && level < 10) {
                        analyzeLevel(child.children, level + 1);
                    }
                }
            }
        };
        
        analyzeLevel(partitionData.partition_tree.tree.children);
        
        // Flatten the levels to maintain hierarchy order
        const orderedColumns = [];
        columnsByLevel.forEach(levelSet => {
            if (levelSet) {
                levelSet.forEach(column => {
                    if (!orderedColumns.includes(column)) {
                        orderedColumns.push(column);
                    }
                });
            }
        });
        
        return orderedColumns;
    }

    async detectTimestampColumn() {
        try {
            // Try a simple query to get column names
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000); // 3 second timeout
            
            const response = await fetch(this.queryApiUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain',
                },
                body: `SELECT * FROM ${this.selectedTable} LIMIT 1`,
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            if (response.ok) {
                const result = await response.text();
                const lines = result.trim().split('\n');
                if (lines.length > 0) {
                    const headers = this.parseCsvLine(lines[0]);
                    console.log('Detected columns:', headers);
                    
                    // Common timestamp column patterns in priority order
                    const timestampPatterns = [
                        { pattern: /^ts_ms$/i, priority: 1 },
                        { pattern: /^timestamp$/i, priority: 2 },
                        { pattern: /^ts$/i, priority: 3 },
                        { pattern: /^time$/i, priority: 4 },
                        { pattern: /^event_time$/i, priority: 5 },
                        { pattern: /^created_at$/i, priority: 6 },
                        { pattern: /^datetime$/i, priority: 7 },
                        { pattern: /.*_ts$/i, priority: 8 },
                        { pattern: /.*timestamp.*/i, priority: 9 },
                        { pattern: /.*time.*/i, priority: 10 },
                        { pattern: /.*date.*/i, priority: 11 }
                    ];
                    
                    // Find the best matching timestamp column
                    let bestMatch = null;
                    let bestPriority = Infinity;
                    
                    for (const { pattern, priority } of timestampPatterns) {
                        const match = headers.find(header => pattern.test(header));
                        if (match && priority < bestPriority) {
                            bestMatch = match;
                            bestPriority = priority;
                        }
                    }
                    
                    if (bestMatch) {
                        console.log('Detected timestamp column:', bestMatch);
                        return bestMatch;
                    }
                }
            }
        } catch (error) {
            console.log('Could not detect timestamp column from schema:', error);
        }
        
        return '';
    }

    determineTimestampFormat(hiveColumns) {
        // Check if we have time-based partitions
        const hasYear = hiveColumns.includes('year');
        const hasMonth = hiveColumns.includes('month');
        const hasDay = hiveColumns.includes('day');
        const hasHour = hiveColumns.includes('hour');
        
        // Determine timestamp format based on partition granularity
        if (hasHour) {
            return 'hour';
        } else if (hasDay) {
            return 'day';
        } else if (hasMonth) {
            return 'month';
        }
        
        return 'day';
    }

    hideCompactDialog() {
        this.compactDialog.style.display = 'none';
        document.body.style.overflow = '';
    }


    async executeCompact() {
        const originalText = this.compactExecute.innerHTML;
        this.compactExecute.disabled = true;
        this.compactExecute.innerHTML = 'Starting...';
        
        try {
            // Build parameters - request async mode
            const params = new URLSearchParams({
                table: this.selectedTable,
                target_file_size_mb: this.targetFileSize.value,
                async: 'true'
            });

            const url = `${this.compactApiUrl}?${params.toString()}`;
            const response = await fetch(url, {
                method: 'POST'
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

            const result = await response.json();
            console.log('Compact started:', result);
            
            // Hide the dialog
            this.hideCompactDialog();
            
            // Switch to tasks tab and monitor the task
            this.switchTab('tasks');
            await this.loadTasks();

        } catch (error) {
            console.error('Compact error:', error);
            this.showStatus(`Compact failed: ${error.message}`, 'error');
            this.compactExecute.innerHTML = originalText;
            this.compactExecute.disabled = false;
        }
    }
    

    async showRepartitionDialog() {
        if (!this.selectedTable) {
            this.showStatus('Please select a table to repartition', 'error');
            return;
        }

        // Set table name and show modal immediately
        this.repartitionTableName.value = this.selectedTable;
        this.repartitionDialog.style.display = 'flex';
        document.body.style.overflow = 'hidden';
        
        // Load data in background and update form
        this.loadRepartitionFormData();
    }

    async loadRepartitionFormData() {
        try {
            // Show loading state
            this.setRepartitionDefaults();
            this.repartitionHiveColumns.value = 'Loading...';
            this.repartitionTimestampColumn.value = 'Detecting...';
            this.repartitionHiveColumns.disabled = true;
            this.repartitionTimestampColumn.disabled = true;
            
            // Load partition data and detect timestamp in parallel
            const [partitionData, timestampColumn] = await Promise.all([
                this.loadPartitionDetails(this.selectedTable),
                this.detectTimestampColumn()
            ]);
            
            await this.prefillRepartitionForm(partitionData, timestampColumn);
        } catch (error) {
            console.log('Could not load repartition form data:', error);
            this.setRepartitionDefaults();
        } finally {
            this.repartitionHiveColumns.disabled = false;
            this.repartitionTimestampColumn.disabled = false;
        }
    }

    async prefillRepartitionForm(partitionData, detectedTimestampColumn) {
        // Default target file size
        this.repartitionTargetFileSize.value = '128';
        
        // Analyze partition structure
        const isPartitioned = partitionData && 
                             partitionData.partition_tree && 
                             partitionData.partition_tree.summary && 
                             partitionData.partition_tree.summary.is_partitioned;
        
        if (isPartitioned) {
            // Try to detect partition columns from the tree structure
            const hiveColumns = this.detectHiveColumns(partitionData);
            this.repartitionHiveColumns.value = hiveColumns.join(', ');
        } else {
            // Non-partitioned table - suggest a basic partition scheme
            this.repartitionHiveColumns.value = '';
        }
        
        // Use the detected timestamp column and determine format
        this.repartitionTimestampColumn.value = detectedTimestampColumn || '';
        this.repartitionTimestampFormat.value = 'day';
    }

    setRepartitionDefaults() {
        this.repartitionTargetFileSize.value = '128';
        this.repartitionHiveColumns.value = '';
        this.repartitionTimestampColumn.value = '';
        this.repartitionTimestampFormat.value = 'day';
    }

    hideRepartitionDialog() {
        this.repartitionDialog.style.display = 'none';
        document.body.style.overflow = '';
    }

    async executeRepartition() {
        const originalText = this.repartitionExecute.innerHTML;
        this.repartitionExecute.disabled = true;
        this.repartitionExecute.innerHTML = 'Starting...';
        
        try {
            // Build parameters
            const params = new URLSearchParams({
                table: this.selectedTable,
                target_file_size_mb: this.repartitionTargetFileSize.value,
                async: 'true'  // Request async mode
            });

            // Add optional parameters
            if (this.repartitionHiveColumns.value.trim()) {
                params.append('hive_columns', this.repartitionHiveColumns.value.trim());
            }
            if (this.repartitionTimestampColumn.value.trim()) {
                params.append('timestamp_column', this.repartitionTimestampColumn.value.trim());
                params.append('timestamp_format', this.repartitionTimestampFormat.value);
            }

            // Start the repartition job
            const url = `${this.queryApiUrl.replace('/query', '/repartition')}?${params.toString()}`;
            const response = await fetch(url, {
                method: 'POST'
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `HTTP ${response.status}`);
            }

            const result = await response.json();
            console.log('Repartition started:', result);
            
            // Hide the dialog
            this.hideRepartitionDialog();
            
            // Switch to tasks tab and monitor the task
            this.switchTab('tasks');
            await this.loadTasks();

        } catch (error) {
            console.error('Repartition error:', error);
            this.showStatus(`Repartition failed: ${error.message}`, 'error');
            this.repartitionExecute.innerHTML = originalText;
            this.repartitionExecute.disabled = false;
        }
    }



    clearQuery() {
        this.sqlEditor.value = '';
        this.sqlEditor.focus();
    }

    showStatus(message, type = '') {
        this.queryStatus.textContent = message;
        this.queryStatus.className = `status ${type}`;
    }

    // Add method to load partition details for a specific table (full tree for analysis)
    async loadPartitionDetails(tableName) {
        try {
            // For the info dialog, use lazy loading with the selected path if available
            let url = `${this.queryApiUrl.replace('/query', '/partitions')}?table=${encodeURIComponent(tableName)}`;
            
            // If we have a selected partition path, load just that level
            if (this.selectedPartitionPath) {
                url += `&path=${encodeURIComponent(this.selectedPartitionPath)}`;
                // Include sizes - now optimized to only scan direct children
                url += '&include_sizes=true';
            } else {
                // For root level, include sizes as it's usually manageable
                url += '&path=';
                url += '&include_sizes=true';
            }
            
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            console.log('Partition details for', tableName, data);
            return data;
        } catch (error) {
            console.error('Error loading partition details:', error);
            return null;
        }
    }

    async loadPartitionInfo(tableName) {
        try {
            const response = await fetch(`${this.partitionInfoApiUrl}?table=${encodeURIComponent(tableName)}`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            console.log('Partition info for', tableName, data);
            return data;
        } catch (error) {
            console.error('Error loading partition info:', error);
            return null;
        }
    }

    setupResizer() {
        let isResizing = false;
        
        this.resizer.addEventListener('mousedown', () => {
            isResizing = true;
            document.body.style.cursor = 'row-resize';
            document.body.style.userSelect = 'none';
        });

        document.addEventListener('mousemove', (e) => {
            if (!isResizing) return;

            const container = document.querySelector('.main-content');
            const containerRect = container.getBoundingClientRect();
            const querySection = document.querySelector('.query-section');
            
            const mouseY = e.clientY - containerRect.top;
            
            // Calculate new height for query section (minimum 150px, maximum container height - 200px for results)
            const minQueryHeight = 150;
            const maxQueryHeight = containerRect.height - 200 - 4; // Reserve 200px for results + 4px for resizer
            
            let newQueryHeight = Math.max(minQueryHeight, Math.min(mouseY - 16, maxQueryHeight)); // 16px for padding
            
            querySection.style.flexBasis = `${newQueryHeight}px`;
        });

        document.addEventListener('mouseup', () => {
            if (isResizing) {
                isResizing = false;
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            }
        });
    }

    setupHorizontalResizer() {
        let isResizing = false;
        
        this.horizontalResizer.addEventListener('mousedown', (e) => {
            isResizing = true;
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            e.preventDefault();
        });

        document.addEventListener('mousemove', (e) => {
            if (!isResizing) return;

            const containerRect = document.querySelector('.container').getBoundingClientRect();
            const mouseX = e.clientX - containerRect.left;
            
            // Calculate new width for sidebar (minimum 200px, maximum 600px)
            const minSidebarWidth = 200;
            const maxSidebarWidth = 600;
            const containerWidth = containerRect.width;
            const maxAllowedWidth = Math.min(maxSidebarWidth, containerWidth * 0.7); // Max 70% of container
            
            let newSidebarWidth = Math.max(minSidebarWidth, Math.min(mouseX - 4, maxAllowedWidth)); // 4px for resizer width
            
            this.sidebar.style.width = `${newSidebarWidth}px`;
        });

        document.addEventListener('mouseup', () => {
            if (isResizing) {
                isResizing = false;
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            }
        });

        // Handle double-click to reset width
        this.horizontalResizer.addEventListener('dblclick', () => {
            this.sidebar.style.width = '300px';
        });
    }

    
    replaceFromClauses() {
        if (!this.selectedTable) return;
        
        let query = this.sqlEditor.value;
        const fromPattern = /FROM\s+([^\s]+)/g;
        
        const newQuery = query.replace(fromPattern, () => {
            // Always use the simple table name - the backend will handle schema resolution
            return `FROM ${this.selectedTable}`;
        });
        
        if (newQuery !== query) {
            this.sqlEditor.value = newQuery;
        }
    }

    showQueryTiming(totalTime, serverTime, dataTransferTime, result) {
        const dataSize = result.length;
        const dataSizeKB = (dataSize / 1024).toFixed(1);
        
        // Show breakdown: Server processing time vs pure data transfer time
        this.queryTiming.textContent = `Server: ${serverTime.toFixed(0)}ms | Transfer: ${dataTransferTime.toFixed(0)}ms | Total: ${totalTime.toFixed(0)}ms | Data: ${dataSizeKB}KB`;
    }
    
    switchTab(tabName) {
        // Update tab buttons
        document.querySelectorAll('.tab-button').forEach(button => {
            button.classList.toggle('active', button.dataset.tab === tabName);
        });
        
        // Update tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.toggle('active', content.dataset.tab === tabName);
        });
    }
    
    displayExplainOutput(explainText) {
        if (!explainText) {
            this.explainContent.textContent = 'No execution plan available';
            return;
        }
        
        // Format the explain output for better display
        let formattedExplain = explainText;
        
        // Highlight different components
        // Operators (words in all caps)
        formattedExplain = formattedExplain.replace(/\b[A-Z_]+(?:\s+[A-Z_]+)*\b/g, match => {
            if (match.length > 3 && !match.includes('HTTP') && !match.includes('GET')) {
                return `<span class="operator">${match}</span>`;
            }
            return match;
        });
        
        // Highlight important metrics
        formattedExplain = formattedExplain.replace(/(Scanning Files:|Total Files Read:|File Filters:|#GET:|#HEAD:|in:|out:)([^\n]*)/g, 
            '<span style="color: #d73a49; font-weight: bold;">$1</span><span style="color: #005cc5;">$2</span>');
        
        // Highlight row counts
        formattedExplain = formattedExplain.replace(/(\d+\s+Rows)/g, 
            '<span style="color: #6f42c1; font-weight: bold;">$1</span>');
        
        // Highlight performance metrics
        formattedExplain = formattedExplain.replace(/(\d+\.\d+\s*[KMG]iB|\d+\s*bytes)/g, 
            '<span style="color: #e36209; font-weight: bold;">$1</span>');
        
        // Add summary box if we find key metrics
        const fileMatch = explainText.match(/Scanning Files:\s*(\d+)\/(\d+)/);
        const getMatch = explainText.match(/#GET:\s*(\d+)/);
        const dataMatch = explainText.match(/in:\s*([\d.]+\s*[KMG]?i?B|\d+\s*bytes)/);
        
        if (fileMatch || getMatch || dataMatch) {
            let summary = '<div style="background: #f6f8fa; border: 1px solid #d1d5da; border-radius: 6px; padding: 12px; margin-bottom: 16px;">';
            summary += '<h4 style="margin: 0 0 8px 0; color: #24292e;">Query Statistics:</h4>';
            summary += '<ul style="margin: 0; padding-left: 20px; color: #586069;">';
            
            if (fileMatch) {
                summary += `<li><strong>Files Scanned:</strong> ${fileMatch[1]} out of ${fileMatch[2]} total files</li>`;
            }
            if (getMatch) {
                summary += `<li><strong>S3 GET Requests:</strong> ${getMatch[1]}</li>`;
            }
            if (dataMatch) {
                summary += `<li><strong>Data Downloaded:</strong> ${dataMatch[1]}</li>`;
            }
            
            summary += '</ul></div>';
            formattedExplain = summary + formattedExplain;
        }
        
        // Display the formatted plan
        this.explainContent.innerHTML = formattedExplain;
    }


    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    arraysEqual(arr1, arr2) {
        if (!arr1 || !arr2) return false;
        if (arr1.length !== arr2.length) return false;
        return arr1.every((val, index) => val === arr2[index]);
    }

    // Waveform visualization methods
    updateWaveformColumns() {
        if (!this.queryData || !this.queryData.headers) return;
        
        // Clear existing options
        this.xAxisColumn.innerHTML = '<option value="">Select column...</option>';
        this.yAxisColumns.innerHTML = '';
        
        // Add column options
        this.queryData.headers.forEach(header => {
            // X-axis option
            const xOption = document.createElement('option');
            xOption.value = header;
            xOption.textContent = header;
            this.xAxisColumn.appendChild(xOption);
            
            // Y-axis option
            const yOption = document.createElement('option');
            yOption.value = header;
            yOption.textContent = header;
            this.yAxisColumns.appendChild(yOption);
        });
    }
    
    plotWaveform() {
        console.log('Plot button clicked');
        
        if (!this.queryData || !this.queryData.rows || this.queryData.rows.length === 0) {
            alert('No data available for plotting. Please run a query first.');
            return;
        }
        
        const xColumnName = this.xAxisColumn.value;
        const yColumnNames = Array.from(this.yAxisColumns.selectedOptions)
            .map(opt => opt.value)
            .filter(val => val && val.trim() !== '');
        
        if (!xColumnName) {
            alert('Please select an X-axis column.');
            return;
        }
        
        if (yColumnNames.length === 0) {
            alert('Please select at least one Y-axis column.');
            return;
        }
        
        // Get column indices
        const xIndex = this.queryData.headers.indexOf(xColumnName);
        const yIndices = yColumnNames.map(name => this.queryData.headers.indexOf(name));
        
        // Extract data and detect if x-axis is time-based
        const isTimeColumn = xColumnName.toLowerCase().includes('time') || 
                           xColumnName.toLowerCase().includes('ts') || 
                           xColumnName.toLowerCase().includes('date');
        
        const xData = this.queryData.rows.map(row => {
            const val = row[xIndex];
            
            // Handle timestamp columns
            if (isTimeColumn) {
                // Check if it's a numeric timestamp (milliseconds or seconds)
                const num = parseFloat(val);
                if (!isNaN(num)) {
                    // Convert seconds to milliseconds if needed
                    if (num < 1e12) {
                        return new Date(num * 1000);
                    } else {
                        return new Date(num);
                    }
                }
                // Try parsing as date string
                const date = new Date(val);
                if (!isNaN(date.getTime())) {
                    return date;
                }
            }
            
            // Default: try to parse as number, otherwise use as string
            const num = parseFloat(val);
            return isNaN(num) ? val : num;
        });
        
        // Create datasets for each Y column
        const datasets = yColumnNames.map((name, i) => {
            const yIndex = yIndices[i];
            const yData = this.queryData.rows.map(row => {
                const val = row[yIndex];
                return parseFloat(val) || 0;
            });
            
            // Generate color for this dataset
            const colors = ['#0066ff', '#ff6b00', '#00a86b', '#ff0066', '#6b00ff', '#00ffa8'];
            const color = colors[i % colors.length];
            
            return {
                label: name,
                data: yData,
                borderColor: color,
                backgroundColor: color + '20',
                tension: 0.1,
                pointRadius: 3,
                pointHoverRadius: 5,
                borderWidth: 2,
                fill: false,
                stepped: false
            };
        });
        
        // Destroy existing chart if it exists
        if (this.waveformChart) {
            this.waveformChart.destroy();
        }
        
        // Create new chart
        const ctx = document.getElementById('waveform-chart').getContext('2d');
        
        // Configure x-axis based on data type
        const xAxisConfig = {
            display: true,
            title: {
                display: true,
                text: xColumnName
            }
        };
        
        // If x-axis contains Date objects, use time scale
        if (xData.length > 0 && xData[0] instanceof Date) {
            xAxisConfig.type = 'time';
            xAxisConfig.time = {
                displayFormats: {
                    millisecond: 'HH:mm:ss.SSS',
                    second: 'HH:mm:ss',
                    minute: 'HH:mm',
                    hour: 'MMM dd HH:mm',
                    day: 'MMM dd',
                    week: 'MMM dd',
                    month: 'MMM yyyy',
                    quarter: 'MMM yyyy',
                    year: 'yyyy'
                }
            };
        }
        
        // Create datasets with x,y format for time series
        const chartDatasets = datasets.map(dataset => {
            if (xData.length > 0 && xData[0] instanceof Date) {
                // For time series, use {x, y} format
                return {
                    ...dataset,
                    data: dataset.data.map((y, i) => ({ x: xData[i], y }))
                };
            } else {
                // For regular data, keep as is
                return dataset;
            }
        });
        
        this.waveformChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: (xData.length > 0 && xData[0] instanceof Date) ? undefined : xData,
                datasets: chartDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    title: {
                        display: true,
                        text: 'Query Results Waveform'
                    },
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    x: xAxisConfig,
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: yColumnNames.length === 1 ? yColumnNames[0] : 'Values'
                        }
                    }
                },
                elements: {
                    line: {
                        borderWidth: 2
                    },
                    point: {
                        radius: 3
                    }
                }
            }
        });
    }
    
    // Task Management Methods
    async loadTasks() {
        try {
            const response = await fetch(this.tasksApiUrl);
            if (!response.ok) throw new Error(`Failed to load tasks: ${response.status}`);
            
            const data = await response.json();
            this.displayTasks(data.tasks);
            
            // Start monitoring active tasks
            data.tasks.forEach(task => {
                if (['running', 'paused'].includes(task.status)) {
                    this.monitorTask(task.id);
                }
            });
        } catch (error) {
            console.error('Error loading tasks:', error);
            this.tasksList.innerHTML = `
                <div style="text-align: center; color: #d93025; padding: 20px;">
                    Error loading tasks: ${error.message}
                </div>
            `;
        }
    }
    
    displayTasks(tasks) {
        if (!tasks || tasks.length === 0) {
            this.tasksList.innerHTML = `
                <div class="no-tasks" style="text-align: center; color: #5f6368; padding: 40px;">
                    No background tasks
                </div>
            `;
            return;
        }
        
        // Sort tasks by created_at desc
        tasks.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
        
        this.tasksList.innerHTML = tasks.map(task => this.createTaskCard(task)).join('');
        
        // Bind action buttons
        tasks.forEach(task => {
            this.bindTaskActions(task.id);
        });
    }
    
    createTaskCard(task) {
        const statusClass = task.status.toLowerCase();
        const canStart = ['pending', 'paused'].includes(task.status);
        const canPause = task.status === 'running';
        const canResume = task.status === 'paused';
        const canCancel = ['pending', 'running', 'paused'].includes(task.status);
        const canDelete = ['completed', 'failed', 'cancelled'].includes(task.status);
        
        return `
            <div class="task-card" id="task-${task.id}">
                <div class="task-header">
                    <div>
                        <h4 class="task-title">${this.getTaskTitle(task)}</h4>
                        <div class="task-type">${task.type}</div>
                    </div>
                    <span class="task-status ${statusClass}">${task.status}</span>
                </div>
                
                ${task.status === 'running' || task.status === 'paused' ? `
                    <div class="task-progress">
                        <div class="task-progress-bar">
                            <div class="task-progress-fill" style="width: ${task.progress}%"></div>
                        </div>
                    </div>
                ` : ''}
                
                <div class="task-message">${task.message || ''}</div>
                
                ${task.error ? `
                    <div style="color: #d93025; font-size: 13px; margin-top: 8px;">
                        Error: ${task.error}
                    </div>
                ` : ''}
                
                <div class="task-actions">
                    ${canStart && !canResume ? `
                        <button class="task-action-btn primary" onclick="queryConsole.startTask('${task.id}')">
                            Start
                        </button>
                    ` : ''}
                    
                    ${canPause ? `
                        <button class="task-action-btn" onclick="queryConsole.pauseTask('${task.id}')">
                            Pause
                        </button>
                    ` : ''}
                    
                    ${canResume ? `
                        <button class="task-action-btn primary" onclick="queryConsole.resumeTask('${task.id}')">
                            Resume
                        </button>
                    ` : ''}
                    
                    ${canCancel ? `
                        <button class="task-action-btn" onclick="queryConsole.cancelTask('${task.id}')">
                            Cancel
                        </button>
                    ` : ''}
                    
                    ${canDelete ? `
                        <button class="task-action-btn" onclick="queryConsole.deleteTask('${task.id}')">
                            Delete
                        </button>
                    ` : ''}
                    
                    ${task.status === 'completed' && task.result ? `
                        <button class="task-action-btn" onclick="queryConsole.viewTaskResult('${task.id}')">
                            View Result
                        </button>
                    ` : ''}
                </div>
                
                <div class="task-timestamp">
                    Created: ${new Date(task.created_at).toLocaleString()}
                    ${task.completed_at ? `<br>Completed: ${new Date(task.completed_at).toLocaleString()}` : ''}
                </div>
            </div>
        `;
    }
    
    getTaskTitle(task) {
        switch (task.type) {
            case 'repartition':
                return `Repartition ${task.params.table_name}`;
            case 'compact':
                return `Compact ${task.params.table_name}`;
            default:
                return `${task.type} Task`;
        }
    }
    
    bindTaskActions(taskId) {
        // Actions are bound via onclick in the HTML
    }
    
    async startTask(taskId) {
        try {
            const response = await fetch(`${this.tasksApiUrl}/${taskId}/start`, { method: 'POST' });
            if (!response.ok) throw new Error('Failed to start task');
            
            this.monitorTask(taskId);
            await this.loadTasks();
        } catch (error) {
            console.error('Error starting task:', error);
            this.showStatus(`Failed to start task: ${error.message}`, 'error');
        }
    }
    
    async pauseTask(taskId) {
        try {
            const response = await fetch(`${this.tasksApiUrl}/${taskId}/pause`, { method: 'POST' });
            if (!response.ok) throw new Error('Failed to pause task');
            
            await this.loadTasks();
        } catch (error) {
            console.error('Error pausing task:', error);
            this.showStatus(`Failed to pause task: ${error.message}`, 'error');
        }
    }
    
    async resumeTask(taskId) {
        try {
            const response = await fetch(`${this.tasksApiUrl}/${taskId}/resume`, { method: 'POST' });
            if (!response.ok) throw new Error('Failed to resume task');
            
            await this.loadTasks();
        } catch (error) {
            console.error('Error resuming task:', error);
            this.showStatus(`Failed to resume task: ${error.message}`, 'error');
        }
    }
    
    async cancelTask(taskId) {
        if (!confirm('Are you sure you want to cancel this task?')) return;
        
        try {
            const response = await fetch(`${this.tasksApiUrl}/${taskId}/cancel`, { method: 'POST' });
            if (!response.ok) throw new Error('Failed to cancel task');
            
            // Close event source if monitoring
            if (this.taskEventSources.has(taskId)) {
                this.taskEventSources.get(taskId).close();
                this.taskEventSources.delete(taskId);
            }
            
            await this.loadTasks();
        } catch (error) {
            console.error('Error cancelling task:', error);
            this.showStatus(`Failed to cancel task: ${error.message}`, 'error');
        }
    }
    
    async deleteTask(taskId) {
        if (!confirm('Are you sure you want to delete this task?')) return;
        
        try {
            const response = await fetch(`${this.tasksApiUrl}/${taskId}/delete`, { method: 'DELETE' });
            if (!response.ok) throw new Error('Failed to delete task');
            
            await this.loadTasks();
        } catch (error) {
            console.error('Error deleting task:', error);
            this.showStatus(`Failed to delete task: ${error.message}`, 'error');
        }
    }
    
    viewTaskResult(taskId) {
        const task = this.activeTasks.get(taskId);
        if (task && task.result) {
            // Display result in the results tab
            this.switchTab('results');
            this.displayTaskResult(task);
        }
    }
    
    displayTaskResult(task) {
        const result = task.result;
        let html = `
            <div style="padding: 20px;">
                <h3>${this.getTaskTitle(task)} - Results</h3>
                <div style="margin-top: 16px;">
        `;
        
        if (task.type === 'repartition') {
            html += `
                <div><strong>Rows Processed:</strong> ${result.rows_processed?.toLocaleString() || 0}</div>
                <div><strong>New Partition Structure:</strong> ${result.new_hive_columns?.join(', ') || 'None'}</div>
            `;
        } else if (task.type === 'compact') {
            html += `
                <div><strong>Files Before:</strong> ${result.files_before || 0}</div>
                <div><strong>Files After:</strong> ${result.files_after || 0}</div>
                <div><strong>Size Before:</strong> ${result.size_before_mb?.toFixed(1) || 0} MB</div>
                <div><strong>Size After:</strong> ${result.size_after_mb?.toFixed(1) || 0} MB</div>
                <div><strong>Compression Ratio:</strong> ${result.compression_ratio?.toFixed(2) || 1}x</div>
            `;
        }
        
        html += `
                </div>
            </div>
        `;
        
        this.resultsContainer.innerHTML = html;
    }
    
    monitorTask(taskId) {
        // Close existing event source if any
        if (this.taskEventSources.has(taskId)) {
            this.taskEventSources.get(taskId).close();
        }
        
        const eventSource = new EventSource(`${this.tasksApiUrl}/${taskId}/progress`);
        this.taskEventSources.set(taskId, eventSource);
        
        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            
            // Update task in memory
            if (data.id) {
                this.activeTasks.set(taskId, data);
            }
            
            // Update UI
            this.updateTaskCard(taskId, data);
            
            // Close connection if task is complete
            if (['completed', 'failed', 'cancelled'].includes(data.status)) {
                eventSource.close();
                this.taskEventSources.delete(taskId);
                
                // Reload tasks to ensure consistency
                setTimeout(() => this.loadTasks(), 1000);
            }
        };
        
        eventSource.onerror = () => {
            console.error(`Lost connection to task ${taskId}`);
            eventSource.close();
            this.taskEventSources.delete(taskId);
        };
    }
    
    updateTaskCard(taskId, update) {
        const card = document.getElementById(`task-${taskId}`);
        if (!card) return;
        
        // Update status
        const statusEl = card.querySelector('.task-status');
        if (statusEl && update.status) {
            statusEl.textContent = update.status;
            statusEl.className = `task-status ${update.status.toLowerCase()}`;
        }
        
        // Update progress
        if (update.progress !== undefined) {
            const progressFill = card.querySelector('.task-progress-fill');
            if (progressFill) {
                progressFill.style.width = `${update.progress}%`;
            }
        }
        
        // Update message
        const messageEl = card.querySelector('.task-message');
        if (messageEl && update.message) {
            messageEl.textContent = update.message;
        }
        
        // Show notification for completion
        if (update.status === 'completed') {
            this.showStatus(`✅ ${this.getTaskTitle({type: update.type || 'Task', params: update.params || {}})} completed successfully!`, 'success');
        } else if (update.status === 'failed' || update.status === 'error') {
            this.showStatus(`❌ ${this.getTaskTitle({type: update.type || 'Task', params: update.params || {}})} failed: ${update.error || 'Unknown error'}`, 'error');
        }
    }
}

// Store instance globally for onclick handlers
let queryConsole;

// Initialize the console when the page loads
document.addEventListener('DOMContentLoaded', () => {
    queryConsole = new QueryConsole();
});
