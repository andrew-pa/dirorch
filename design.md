# Dirorch

Manages a generic directory-based workflow process.
A workflow is defined using a YAML file.

## Model
Workflows are comprised of phases, where each phase has a set of states.
Workflow entities are assigned to a specific phase and state.
Each phase also defines transition hooks which run as entities move from a defined source state to a destination state.
A transition hook can also indicate that after the hook runs successfully the process should immediately jump to some other phase and run that phase until fixpoint, and then return to processing the current phase.
Phases are executed one at a time. Each phase's transitions are applied until there are no entities left to apply (ie fixpoint), and then the next phase is processed.
A phase can also define a completion hook which runs after the phase has reached fixpoint but before moving to the next phase.
When all phases complete, the process returns to the first phase. If at this point the first phase immediately reachs fixpoint without taking any transitions, exit successfully.

A workflow definition should also have an optional section for environment variables which are passed to all hook shells.

## Implementation
We model the workflow's overall state on disk using one directory per phase, with child directories per state, and each entity as a file.
When first starting, the necessary directories are created if they do not exist. The system then beings or resumes processing at the currently running phase.

Transition hooks are shell commands executed with the environment variable `INPUT_ENTITY` set to the path of the entity in the source state, and environment variables `DIR_<PHASE>_<STATE>` defined for each phase state respectively pointing to their directories.
When the transition hook completes successfully the entity file is moved to the destination state directory. 
If it is not successful, it is retried N times then if it still fails the task is moved to a special failure state (`_failed`) and no jump is taken.
Entities should be processed in the ordering of their filenames alphabetically.
If a group of entities have `XX-name.whatever` names where `XX` is a number and the number is the same for all entities in the group, their hooks may be executed concurrently.
Otherwise hooks should run sequentially.

Completion hooks are passed the `DIR_<PHASE>_<STATE>` environment variables and also are retried on failure.

## Example Workflow
```yaml
phases:
    tasks:
      states:
          - new
          - in_progress
          - complete
      transitions:
          - from: new
            to: in_progress
            cmd: "./plan-task $INPUT_ENTITY $DIR_SUBTASKS_NEW"
            jump: subtasks
          - from: in_progress
            to: complete
      completions:
          - cmd: "./generate-tasks $DIR_TASKS_NEW"
    subtasks:
        states:
            - new
            - complete
        transitions:
            - from: new
              to: complete
              cmd: "./exec-subtask $INPUT_ENTITY"
```

# Guidelines
- Follow best practices and the software design principles.
- Write robust, readable code that handles errors nicely (including reporting) and is configurable.
- Make sure to provide logging so users can track processing progress.

### Software Design Principles
- SOLID:
    **Single Responsibility Principle** — each module or class has exactly one reason to change.
    **Open/Closed Principle** — systems should be extendable without modifying existing code.
    **Liskov Substitution Principle** — subtypes must be safely interchangeable with their base types.
    **Interface Segregation Principle** — clients depend only on the interfaces they actually use.
    **Dependency Inversion Principle** — high-level logic depends on abstractions, not concrete details.
- **Don't repeat yourself** -- avoid repetitive code and always reuse existing code if possible.
- Functions should not have more than three arguments (not counting `self`). If more data needs passed to a function, consider abstractions (ie objects/closures). 
- Use the most up-to-date versions of packages and interfaces.
- Use `asyncio` and concurrent programming.
